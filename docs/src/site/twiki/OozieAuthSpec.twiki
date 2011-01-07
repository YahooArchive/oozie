<noautolink>

[[index][::Go back to Oozie Documentation Index::]]

-----

---+!! Oozie Authentication Specification

The goal of this document is to provide developer a tutorial of how to write your own authentication and configure in Oozie.

%TOC%


---++ 0 Oozie Authentication Definitions

*Authenticator:* A client side class to authenticate user and send the authentication information to server along with each request.

*AuthenticationProvider:* A server side component to retrieve authentication token from http request and validate the token.

*AuthenticationToken:* A object contains authentication information for a request.

---++ 1 Oozie Authentication Introduction

Oozie Authentication provides a framework to let developer provide a custom implementation to authenticate the requests from
Oozie client. The client side authentication code is used to send the authentication information as a header in the HTTP request
sent to the server. It can be used to send different kinds of authentication tokens or certificates. After a successful
authentication using one of the configured methods, it sends Hadoop-HTTP-Auth cookie in further requests.

The server side authentication module has a AuthenticationProviderFactory which needs to be initialized with the required
AuthenticationProviders from a configuration file. The authentication is handled by the AuthenticationProcessingFilter.
Once a request is received the following happens in the filter:

    * Request is checked for presence of Hadoop-HTTP-Auth cookie. If present the signature is verified and 
      the username is extracted from the cookie.
    * If the cookie is not present or is invalid, a supported provider is fetched from the AuthenticationProviderFactory
      passing in the Request. If there is no supported provider a 401 is sent with the header "WWW-Authenticate: Negotiate"
    * If a supported provider is found, the authentication is delegated to the supported provider. On successful authentication
      the provider returns an instance of AuthenticationToken which contains the authenticated user.
    * An UGI object is constructed out of the authenticated user and set as the request attribute "authorized.ugi" which 
      can be later consumed by the servlets. 

---++ 2 Server Authentication Implementation

To write a new custom authentication, two classes have to be provided with overrided implementation. 

*Provider:* three methods are required to implement.    
    * supports(): the method checks if its authentication mechanism supports the authentication information a request provided.
    * getAuthenticationToken(): the method is called after supports() returns true and used to constructs the token from parameters in a request. 
    * authenticate(): the method is used to validate a token created above and rewrite it with new information if needed.

*Token:* the instance contains the information for a authentication provider to use.

For example,

SimpleAuthenticationHeadProvider implments the AuthenticationProvider to check if client has send a parameter (username) in the request.

SimpleAuthenticationToken extends AbstractAuthenticationToken to set the authentication flag to true and save client parameter in a instance of Token.

---++ 3 Client Authentication Implementation


---++ 4 Server Authentication Configuration

An authentication provider can be given in 'oozie-site.xml' for Oozie server. The property 'authentication.providers' is used to configure
what authentication mechanisms are supported in Oozie server runtime.

   <property>
	<name>authentication.providers</name>
	<value>org.apache.hadoop.http.authentication.server.simple.SimpleAuthenticationHeaderProvider</value>
	<description>Comma separated list of authentication providers in FQCN.</description>
   </property>
   
---++ 5 Client Authentication Configuration

