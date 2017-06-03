package com.nitendragautam.project.LogAnalytics;


public class AccessLogRecord {
    private String clientAddress;  //ip address or hostName of the client
    private String clientIdentity; //Generally '-'
    private String remoteUser; //Generally '-'
    private String dateTime;  //Format [day/month/year:hour:second zone]
    private String httpRequest; //'GET /request
    private String httpStatusCode;  //200 ,404 ,500 etc
    private String bytesSent;  //some value or '-
    private String siteReferer;  //Vistors Source
    private String userAgent; //Type of Browser Used

    public AccessLogRecord(String clientAddress ,
                           String clientIdentity ,
                           String remoteUser,
                           String dateTime,
                           String httpRequest,
                           String httpStatusCode,
                           String bytesSent,
                           String siteReferer,
                           String userAgent){
        this.clientAddress = clientAddress;
        this.clientIdentity =clientIdentity;
        this.remoteUser=remoteUser;
        this.dateTime =dateTime;
        this.httpRequest= httpRequest;
        this.httpStatusCode =httpStatusCode;
        this.bytesSent = bytesSent;
        this.siteReferer=siteReferer;
        this.userAgent=userAgent;

    }
    public String getClientAddress() {
        return clientAddress;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    public String getClientIdentity() {
        return clientIdentity;
    }

    public void setClientIdentity(String clientIdentity) {
        this.clientIdentity = clientIdentity;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getHttpRequest() {
        return httpRequest;
    }

    public void setHttpRequest(String httpRequest) {
        this.httpRequest = httpRequest;
    }

    public String getHttpStatusCode() {
        return httpStatusCode;
    }

    public void setHttpStatusCode(String httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public String getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(String bytesSent) {
        this.bytesSent = bytesSent;
    }

    public String getSiteReferer() {
        return siteReferer;
    }

    public void setSiteReferer(String siteReferer) {
        this.siteReferer = siteReferer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
}