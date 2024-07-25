FROM maven:3.9.5-amazoncorretto-17 AS build

COPY src /home/app/src
COPY pom.xml /home/app

RUN mvn -f /home/app/pom.xml clean package -DskipTests

FROM amazoncorretto:17-alpine

RUN apk add --no-cache ffmpeg
RUN chmod 1777 /tmp

COPY --from=build /home/app/target/*.jar /usr/local/lib/app.jar

ENV SPRING_PROFILES_ACTIVE=dev
ENV AZURE_STORAGE_CONTAINER_NAME_TEMP_VIDEO=private-container
ENV AZURE_STORAGE_CONTAINER_NAME_VIDEO=video-container
ENV AZURE_STORAGE_CONTAINER_NAME_IMAGE=image-container
ENV AZURE_STORAGE_CONTAINER_NAME_SUBTITLE=subtitle-container
ENV AZURE_STORAGE_CONTAINER_NAME_KEYWORDS=keywords-container
ENV AZURE_STORAGE_CONTAINER_NAME_PROFILE_IMAGE=profile-image-container
ENV JWT_SECRET=dev-secret-123
ENV APPLICATION_OPENAI_KEY=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
ENV APPLICATION_WHISPER_URL=https://api.openai.com/v1/audio/transcriptions
ENV APPLICATION_GPT_URL=https://api.openai.com/v1/chat/completions
ENV APPLICATION_AUDIO_MAX_SIZE=26214400

ENV AZURE_FRONTDOOR_URL=https://cognitube-hfegcsabcrgybtcw.z01.azurefd.net
ENV APPLICATION_KEYWORD_SERVICE_URL=https://cognitube-keywords-service.azurewebsites.net/api
ENV APPLICATION_TRANSCODING_SERVICE_URL=https://cognitube-transcoding-service.internal.thankfulfield-7c1523f0.eastus.azurecontainerapps.io/api

ENV SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_GOOGLE_CLIENT_ID=906351348030-8h4si5c0r98qsi1610b16sn0itrdchih.apps.googleusercontent.com
ENV SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_GOOGLE_CLIENT_SECRET=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
ENV SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_GOOGLE_SCOPE=email,profile
ENV SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_GOOGLE_REDIRECT_URI=https://cognitube-server-dev.thankfulfield-7c1523f0.eastus.azurecontainerapps.io/login/oauth2/code/google
ENV SPRING_SECURITY_OAUTH2_CLIENT_PROVIDER_GOOGLE_USER_INFO_URI=https://openidconnect.googleapis.com/v1/userinfo
ENV SPRING_SECURITY_OAUTH2_CLIENT_PROVIDER_GOOGLE_TOKEN_URI=https://oauth2.googleapis.com/token
ENV SPRING_SECURITY_OAUTH2_CLIENT_PROVIDER_GOOGLE_AUTHORIZATION_URI=https://accounts.google.com/o/oauth2/v2/auth
ENV APPLICATION_ADMIN_URL=https://cognitube-admin.thankfulfield-7c1523f0.eastus.azurecontainerapps.io

ENV KAFKA_EVENTHUB_NAMESPACE=cognitube-kafka
ENV KAFKA_VIDEO_PROCESS_TOPIC=video-process
ENV KAFKA_VIDEO_PROCESS_GROUP_ID=video-process-group
ENV KAFKA_EVENTHUB_USERNAME=\$ConnectionString

ENTRYPOINT [ "java", "-jar", "/usr/local/lib/app.jar" ]