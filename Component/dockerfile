FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
ARG RESOURCE_REAPER_SESSION_ID="00000000-0000-0000-0000-000000000000"
LABEL "testcontainers.resource-reaper-session"=$RESOURCE_REAPER_SESSION_ID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
ARG RESOURCE_REAPER_SESSION_ID="00000000-0000-0000-0000-000000000000"
LABEL "testcontainers.resource-reaper-session"=$RESOURCE_REAPER_SESSION_ID
WORKDIR /src
COPY ["Component/Component.csproj", "Component/"]
RUN dotnet restore "Component/Component.csproj"
COPY . .
WORKDIR "/src/Component"
RUN dotnet build "Component.csproj" -c Release -o /app/build

FROM build AS Publish
RUN dotnet publish "Component.csproj" -c Release --verbosity=diag -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_ENVIRONMENT=Development
ENTRYPOINT ["dotnet", "Component.dll"]