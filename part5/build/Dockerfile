FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal@sha256:75a2d4226d9b98c914d7eed878a03bef4f365ad548be5d28d464ccba504cec79
COPY aws-emulator.rpm aws-publishing.asc /tmp/
RUN echo '49051da4bd32268e4cbff87690bc05bab7ab342c8e6a71e1803fbf59c0062246  /tmp/aws-emulator.rpm' | sha256sum -c - && rpm --import /tmp/aws-publishing.asc && rpm -K /tmp/aws-emulator.rpm && rpm -i /tmp/aws-emulator.rpm && rm /tmp/aws-emulator.rpm /tmp/aws-publishing.asc && mkdir -p /home/emulator && chown 1000 /home/emulator
USER 1000
ENV HOME=/home/emulator EMULATOR_TELEMETRY=off
EXPOSE 9298
ENTRYPOINT ["aws-emulator","-b","0.0.0.0"]
CMD ["-p","9298"]
