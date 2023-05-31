package com.pilz.examples.hello;

import io.grpc.*;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;

public class AuthInterceptor implements ServerInterceptor {

    public static final String JWT_SIGNING_KEY = "L8hMXsaQOUhk5rg7XPAv4eL34anlCgkMz8CJ0i/8E/0=";
    public static final String LEVEL = "level";
    public static final Context.Key<Integer> LEVEL_CONTEXT_KEY = Context.key(LEVEL);
    public static final Context.Key<String> CLIENT_ID_CONTEXT_KEY = Context.key("clientId");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall,
                                                                 Metadata metadata, ServerCallHandler<ReqT, RespT> next) {


        //Verify that the user is authorized and if they are authorized then get the subject from the jwt
        //and put it in the call context with the key "clientId"
        //Then get the jwt "level" claim and put it in the call context with the key "level"
        //So that the ListenForHello instance can get these values from the context
        //If the user is not authorized then just fail the call without passing it on to ListenForHello
        String value = metadata.get(BearerToken.AUTHORIZATION_METADATA_KEY);
        Status status;
        if (value == null) {
            status = Status.UNAUTHENTICATED.withDescription("Authorization token is missing");
        } else if (!value.startsWith(BearerToken.BEARER_TYPE)) {
            status = Status.UNAUTHENTICATED.withDescription("Unknown authorization type");
        } else {
            try {
                JwtParser parser = Jwts.parser().setSigningKey(JWT_SIGNING_KEY);
                String token = value.substring(BearerToken.BEARER_TYPE.length()).trim();
                Jws<Claims> claims = parser.parseClaimsJws(token);
                final String clientId = claims.getBody().getSubject();
                final Integer level = claims.getBody().get(LEVEL, Integer.class);
                Context ctx = Context.current().withValues(
                        CLIENT_ID_CONTEXT_KEY, clientId,
                        LEVEL_CONTEXT_KEY, level);
                //return next.startCall(serverCall, metadata);
                return Contexts.interceptCall(ctx, serverCall, metadata, next);
            } catch (Exception e) {
                status = Status.UNAUTHENTICATED.withDescription(e.getMessage()).withCause(e);
            }
        }

        serverCall.close(status, metadata);
        return new ServerCall.Listener<>() {
            // noop
        };
    }
}
