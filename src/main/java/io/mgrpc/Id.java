package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;


public class Id {

    private static final Logger log = LoggerFactory.getLogger(Id.class);

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz2345678";

    private static class Holder {
        //In a holder class to defer initialization until needed.
        static final SecureRandom numberGenerator = new SecureRandom();
    }

    private static final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();


    public static SecureRandom getGenerator(){
        return Holder.numberGenerator;
    }

    /**
     * Return a random 10 byte id. It encodes to base32 evenly (16 chars - 5 bits per char). It is valid for topics.
       It is easier than base64UrlSafe to read in logs and match.
       The probability of collision for 10,000 concurrent calls is zero (for 100,000 it is about 4E-15)
     */
    public static String randomId(){
        return randomBase32(10);
    }

    public static String randomBase32(int numBytes){
        byte[] bytes = new byte[numBytes];
        Holder.numberGenerator.nextBytes(bytes);
        return toBase32(bytes);
    }

    /**
     * Return the first 7 chars of id for short debug Strings
     */
    public static String shrt(final String id){
        final int shortLen = 7;
        if(id.length() < shortLen){
            return id;
        }
        return id.substring(0, shortLen);
    }



    public static String base64Uuid(){
        return uuidToBase64Url(UUID.randomUUID());
    }


    public static String uuidToBase64Url(UUID uuid) {
        return new String(base64.encode(uuidToBytes(uuid)));
    }


    public static byte [] uuidToBytes(UUID uuid){
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }



    public static String toBase32(String str) {
        byte[] b = str.getBytes();
        return toBase32(b);
    }

    public static String toBase32(byte[] b) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        for (int i = 0; i < (b.length + 4) / 5; i++) {
            short[] s = new short[5];
            int[] t = new int[8];

            int blocklen = 5;
            for (int j = 0; j < 5; j++) {
                if ((i * 5 + j) < b.length)
                    s[j] = (short) (b[i * 5 + j] & 0xFF);
                else {
                    s[j] = 0;
                    blocklen--;
                }
            }
            int padlen = lenToPadding(blocklen);

            // convert the 5 byte block into 8 characters (values 0-31).

            // upper 5 bits from first byte
            t[0] = (byte) ((s[0] >> 3) & 0x1F);
            // lower 3 bits from 1st byte, upper 2 bits from 2nd.
            t[1] = (byte) (((s[0] & 0x07) << 2) | ((s[1] >> 6) & 0x03));
            // bits 5-1 from 2nd.
            t[2] = (byte) ((s[1] >> 1) & 0x1F);
            // lower 1 bit from 2nd, upper 4 from 3rd
            t[3] = (byte) (((s[1] & 0x01) << 4) | ((s[2] >> 4) & 0x0F));
            // lower 4 from 3rd, upper 1 from 4th.
            t[4] = (byte) (((s[2] & 0x0F) << 1) | ((s[3] >> 7) & 0x01));
            // bits 6-2 from 4th
            t[5] = (byte) ((s[3] >> 2) & 0x1F);
            // lower 2 from 4th, upper 3 from 5th;
            t[6] = (byte) (((s[3] & 0x03) << 3) | ((s[4] >> 5) & 0x07));
            // lower 5 from 5th;
            t[7] = (byte) (s[4] & 0x1F);

            // write out the actual characters.
            for (int j = 0; j < t.length - padlen; j++) {
                char c = ALPHABET.charAt(t[j]);
                os.write(c);
            }
        }
        return new String(os.toByteArray());
    }

    public static String fromBase32(String str) {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        byte[] raw = str.getBytes();
        for (int i = 0; i < raw.length; i++) {
            char c = (char) raw[i];
            if (!Character.isWhitespace(c)) {
                c = Character.toUpperCase(c);
                bs.write((byte) c);
            }
        }

        while (bs.size() % 8 != 0)
            bs.write('8');

        byte[] in = bs.toByteArray();

        bs.reset();
        DataOutputStream ds = new DataOutputStream(bs);

        for (int i = 0; i < in.length / 8; i++) {
            short[] s = new short[8];
            int[] t = new int[5];

            int padlen = 8;
            for (int j = 0; j < 8; j++) {
                char c = (char) in[i * 8 + j];
                if (c == '8')
                    break;
                s[j] = (short) ALPHABET.indexOf(in[i * 8 + j]);
                if (s[j] < 0)
                    return null;
                padlen--;
            }
            int blocklen = paddingToLen(padlen);
            if (blocklen < 0)
                return null;

            // all 5 bits of 1st, high 3 (of 5) of 2nd
            t[0] = (s[0] << 3) | s[1] >> 2;
            // lower 2 of 2nd, all 5 of 3rd, high 1 of 4th
            t[1] = ((s[1] & 0x03) << 6) | (s[2] << 1) | (s[3] >> 4);
            // lower 4 of 4th, high 4 of 5th
            t[2] = ((s[3] & 0x0F) << 4) | ((s[4] >> 1) & 0x0F);
            // lower 1 of 5th, all 5 of 6th, high 2 of 7th
            t[3] = (s[4] << 7) | (s[5] << 2) | (s[6] >> 3);
            // lower 3 of 7th, all of 8th
            t[4] = ((s[6] & 0x07) << 5) | s[7];

            try {
                for (int j = 0; j < blocklen; j++)
                    ds.writeByte((byte) (t[j] & 0xFF));
            } catch (IOException e) {
            }
        }

        return new String(bs.toByteArray());
    }

    private static int lenToPadding(int blocklen) {
        switch (blocklen) {
            case 1:
                return 6;
            case 2:
                return 4;
            case 3:
                return 3;
            case 4:
                return 1;
            case 5:
                return 0;
            default:
                return -1;
        }
    }

    private static int paddingToLen(int padlen) {
        switch (padlen) {
            case 6:
                return 1;
            case 4:
                return 2;
            case 3:
                return 3;
            case 1:
                return 4;
            case 0:
                return 5;
            default:
                return -1;
        }
    }


    public static double collisionProbability(double numberOfObjects, double possibleNumberOfObjects){
        //For probabilities less than 1/2 the probability of a collision can be approximated as
        //p ~= (n*n)/(2*m)
        //Where n is the number of items and m is the number of possibilities for each item.
        return (numberOfObjects*numberOfObjects)/(2*possibleNumberOfObjects);

        //Note: more complex version
        //Given r objects the possibility of collision where there are N possible objects is
        //1-exp(-r**2/(2N))
    }


    public static void main(String[] args){
        log.debug(randomId());
        log.debug("5, 1000 " + collisionProbability(1000, Math.pow(2, 5*8)));
        log.debug("5, 10000 " + collisionProbability(10000, Math.pow(2, 5*8)));
        log.debug("7, 10000 " + collisionProbability(10000, Math.pow(2, 7*8)));
        log.debug("10, 10000 " + collisionProbability(10000, Math.pow(2, 10*8)));
        log.debug("10, 100000 " + collisionProbability(100000, Math.pow(2, 10*8)));
    }



}