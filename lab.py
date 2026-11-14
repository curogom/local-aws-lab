"""Local-only reliability experiments, NOT a cloud throughput benchmark."""
import argparse, collections, concurrent.futures, json, os, pathlib, sqlite3, statistics, time, uuid
ROOT=pathlib.Path(__file__).resolve().parent
RESULTS=ROOT/'results'

def ledger(path):
    db=sqlite3.connect(path, timeout=30)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS results (event_id TEXT PRIMARY KEY, result TEXT NOT NULL)')
    db.commit(); return db

def apply(db, event):
    # Business effect and idempotency record are the SAME transaction in this demo.
    with db:
        c=db.execute('INSERT OR IGNORE INTO results VALUES (?,?)',(event['id'],event['text'].upper()))
    return c.rowcount==1

def sqs_client():
    import boto3
    import configparser
    # Explicit local credentials avoid AWS profiles, environment secrets and metadata.
    runtime=ROOT/'.runtime'/'emulator'/'9298'
    cfg=configparser.ConfigParser()
    if not cfg.read(runtime/'emulator_credentials'):
        raise RuntimeError('Start the dedicated lab emulator first: ./scripts/emulator-start.sh')
    section=cfg['default']
    return boto3.session.Session().client(
        'sqs',endpoint_url='https://127.0.0.1:9298',region_name='us-east-1',
        aws_access_key_id=section['aws_access_key_id'],
        aws_secret_access_key=section['aws_secret_access_key'],
        verify=str(runtime/'ca.pem'))

def sqs_run(n):
    RESULTS.mkdir(exist_ok=True)
    s=sqs_client(); run='qlab-'+uuid.uuid4().hex[:10]; report={'run':run,'kind':'sqs','scope':'AWS Emulator local functional test','cases':[]}
    urls=[]
    def q(suffix, **attrs):
        u=s.create_queue(QueueName=run+'-'+suffix,Attributes={k:str(v) for k,v in attrs.items()})['QueueUrl']; urls.append(u); return u
    def recv(u):
        return s.receive_message(QueueUrl=u,MaxNumberOfMessages=1,WaitTimeSeconds=1,MessageSystemAttributeNames=['All']).get('Messages',[])
    try:
        u=q('visibility',VisibilityTimeout=1)
        s.send_message(QueueUrl=u,MessageBody='visibility-probe')
        first=recv(u)[0]; time.sleep(1.5); again=recv(u)
        passed=bool(again and again[0]['MessageId']==first['MessageId'])
        report['cases'].append({'case':'visibility_expiry_redelivery','pass':passed})
        assert passed, 'Visibility timeout unsupported or inconsistent'
        s.delete_message(QueueUrl=u,ReceiptHandle=again[0]['ReceiptHandle'])
        dlq=q('dlq'); arn=s.get_queue_attributes(QueueUrl=dlq,AttributeNames=['QueueArn'])['Attributes']['QueueArn']
        poison=q('poison',VisibilityTimeout=1,RedrivePolicy=json.dumps({'deadLetterTargetArn':arn,'maxReceiveCount':'2'}))
        s.send_message(QueueUrl=poison,MessageBody='always-fail')
        attempts=0; dead=[]; until=time.monotonic()+12
        while time.monotonic()<until:
            attempts+=len(recv(poison)); time.sleep(1.1)
            dead=recv(dlq)
            if dead: break
        report['cases'].append({'case':'native_dlq_redrive','pass':bool(dead) and attempts==2,'source_deliveries':attempts,'note':'Native broker redrive, not application-side copy'})
        # Failure after a committed effect and before queue acknowledgement.
        u=q('crash',VisibilityTimeout=1); db=ledger(ROOT/'results'/f'{run}-crash.sqlite')
        e={'id':'crash-1','text':'synthetic'}; s.send_message(QueueUrl=u,MessageBody=json.dumps(e))
        msg=recv(u)[0]; applied=apply(db,json.loads(msg['Body']))
        if not applied: raise RuntimeError('First effect was not applied')
        # Simulate process loss by closing state and deliberately not deleting.
        db.close(); time.sleep(1.5); msg=recv(u)[0]; db=ledger(ROOT/'results'/f'{run}-crash.sqlite')
        duplicate_applied=apply(db,json.loads(msg['Body']))
        if duplicate_applied: raise RuntimeError('Duplicate effect was applied')
        s.delete_message(QueueUrl=u,ReceiptHandle=msg['ReceiptHandle'])
        report['cases'].append({'case':'commit_before_ack_recovery','pass':db.execute('SELECT count(*) FROM results').fetchone()[0]==1,'note':'Crash boundary simulated; no OS kill'})
        db.close()
        for workers in [1,2,4]:
            u=q('w'+str(workers),VisibilityTimeout=30)
            records=[{'id':f'doc-{i}','text':'synthetic document '+str(i)} for i in range(n)]
            records+=records[:max(1,n//10)]
            for i in range(0,len(records),10):
                resp=s.send_message_batch(QueueUrl=u,Entries=[{'Id':str(j),'MessageBody':json.dumps(e)} for j,e in enumerate(records[i:i+10])]); assert not resp.get('Failed')
            dbpath=ROOT/'results'/f'{run}-w{workers}.sqlite'; ledger(dbpath).close()
            def worker():
                client=sqs_client(); db=ledger(dbpath); counts=collections.Counter(); lat=[]; empty=0
                while empty<2:
                    msgs=client.receive_message(QueueUrl=u,MaxNumberOfMessages=10,WaitTimeSeconds=1).get('Messages',[])
                    if not msgs: empty+=1; continue
                    empty=0
                    for m in msgs:
                        t=time.perf_counter(); fresh=apply(db,json.loads(m['Body'])); counts['applied' if fresh else 'duplicates']+=1
                        client.delete_message(QueueUrl=u,ReceiptHandle=m['ReceiptHandle']); lat.append((time.perf_counter()-t)*1000)
                db.close(); return counts,lat
            start=time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool: outs=list(pool.map(lambda _:worker(),range(workers)))
            elapsed=time.perf_counter()-start; total=sum((x[0] for x in outs),collections.Counter()); lat=sorted(sum((x[1] for x in outs),[]))
            db=ledger(dbpath); rows=db.execute('SELECT count(*) FROM results').fetchone()[0]; db.close()
            report['cases'].append({'case':'worker_scaling','workers':workers,'input_messages':len(records),'unique_documents':n,'stored_rows':rows,'applied':total['applied'],'duplicates':total['duplicates'],'pass':rows==n and total['applied']==n and sum(total.values())==len(records),'elapsed_s_including_drain_wait':round(elapsed,3),'messages_per_s_local':round(len(records)/elapsed,2),'handler_ack_p95_ms':round(lat[min(len(lat)-1,int(len(lat)*.95))],3)})
        return report
    finally:
        for u in urls:
            s.delete_queue(QueueUrl=u) # only unique synthetic resources created by this run

def kafka_run(n):
    RESULTS.mkdir(exist_ok=True)
    from kafka import KafkaProducer,KafkaConsumer,TopicPartition
    from kafka.admin import KafkaAdminClient,NewTopic
    endpoint='127.0.0.1:19092'; run='qlab-'+uuid.uuid4().hex[:10]; topic=run; group=run+'-workers'
    admin=KafkaAdminClient(bootstrap_servers=endpoint); admin.create_topics([NewTopic(topic,3,1,topic_configs={'retention.ms':'3600000','retention.bytes':'67108864'})]); admin.close()
    producer=KafkaProducer(bootstrap_servers=endpoint,acks='all',value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda v:v.encode())
    records=[{'id':f'doc-{i}','text':'synthetic '+str(i),'seq':i} for i in range(n)]; records+=records[:max(1,n//10)]
    for e in records: producer.send(topic,key=e['id'],value=e).get(timeout=20)
    producer.flush(); producer.close()
    def consumer(g):
        return KafkaConsumer(topic,bootstrap_servers=endpoint,group_id=g,enable_auto_commit=False,auto_offset_reset='earliest',value_deserializer=lambda b:json.loads(b),consumer_timeout_ms=3000,request_timeout_ms=20000,session_timeout_ms=10000)
    c=consumer(group); dbpath=ROOT/'results'/f'{run}.sqlite'; db=ledger(dbpath)
    first=next(c); applied=apply(db,first.value)
    if not applied: raise RuntimeError('First Kafka effect was not applied')
    c.close(autocommit=False); db.close()
    # Broker group committed offset was never advanced. Restart consumes record again.
    c=consumer(group); db=ledger(dbpath); counts=collections.Counter(); start=time.perf_counter(); partitions=collections.Counter()
    for msg in c:
        counts['applied' if apply(db,msg.value) else 'duplicates']+=1; partitions[msg.partition]+=1
    c.commit(); committed={str(p.partition):c.committed(p) for p in c.assignment()}; end={str(p.partition):v for p,v in c.end_offsets(c.assignment()).items()}; c.close()
    rows=db.execute('SELECT count(*) FROM results').fetchone()[0]; db.close()
    # Separate group independently replays the retained log.
    replay=consumer(run+'-audit'); replay_count=sum(1 for _ in replay); replay.close()
    # Explicit seek replay in an assigned consumer: preserves group commit evidence above.
    replay=KafkaConsumer(bootstrap_servers=endpoint,enable_auto_commit=False,consumer_timeout_ms=2000)
    tps=[TopicPartition(topic,i) for i in range(3)]; replay.assign(tps); replay.seek_to_beginning(*tps); seek_count=sum(1 for _ in replay); replay.close()
    result={'run':run,'kind':'kafka','broker':'apache/kafka:4.4.0','scope':'single broker local reliability test, replication factor 1','input_messages':len(records),'unique_documents':n,'first_effect_before_uncommitted_restart':1,'applied_after_restart':counts['applied'],'duplicates_suppressed':counts['duplicates'],'stored_rows':rows,'partition_records':dict(partitions),'committed_offsets':committed,'end_offsets':end,'lag_after_commit':sum(end[k]-committed[k] for k in end),'independent_group_replay':replay_count,'seek_replay':seek_count,'elapsed_s_including_wait_and_replays':round(time.perf_counter()-start,3),'pass':bool(committed) and all(end[k]==committed[k] for k in end) and rows==n and counts['applied']==n-1 and sum(counts.values())==len(records) and replay_count==len(records) and seek_count==len(records),'limitations':['Consumer restart rather than forced process kill','No rebalance test, replication failover or multi-AZ test','Synthetic SQLite effect, not external business transaction']}
    # Delete only this run's UUID topic. Retention alone would not remove metadata.
    admin=KafkaAdminClient(bootstrap_servers=endpoint)
    admin.delete_topics([topic]); admin.close()
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('mode',choices=['sqs','kafka']);p.add_argument('--count',type=int,default=500);args=p.parse_args()
    if not 10<=args.count<=900: p.error('Bounded local test: count must be 10..900')
    result=sqs_run(args.count) if args.mode=='sqs' else kafka_run(args.count)
    out=ROOT/'results'/f"{result['run']}-{args.mode}.json";out.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(out);print(json.dumps(result,ensure_ascii=False,indent=2))
    ok=all(c['pass'] for c in result['cases']) if args.mode=='sqs' else result['pass']
    raise SystemExit(0 if ok else 2)
