package org.example;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

public class Base64Utils {


    public static String randomId(){return randomUuidAsBase64Url();}
    public static String randomUuidAsBase64Url(){
        return uuidToBase64Url(UUID.randomUUID());
    }

    private static final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();

    public static String uuidToBase64Url(UUID uuid) {
        return new String(base64.encode(uuidToBytes(uuid)));
    }


    public static byte [] uuidToBytes(UUID uuid){
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }


    public static void main(String[] args){
        //System.out.println(randomUuidAsBase64Url());
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid.toString());
        System.out.println(uuidToBase64Url(uuid));
    }

}