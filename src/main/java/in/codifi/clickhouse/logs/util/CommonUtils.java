package in.codifi.clickhouse.logs.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.bouncycastle.util.encoders.Hex;

public class CommonUtils {

    private static final String POSITIVE_DECIMAL_REGEX =
            "^([1-9]\\d*|0)(\\.\\d*[1-9])?$|^([1-9]\\d*)\\.0+$|^\\.\\d*[1-9]$";
    private static final String POSITIVE_WHOLENUMBER_REGEX = "^[1-9]\\d*(\\.0+)?$";

    public boolean isPositiveDecimal(String input) {
        String trimmed = input.trim().replaceFirst("^0+(?!$|\\.)", "")
                .replaceAll("(\\.\\d*[^0])0+$", "$1").replaceAll("\\.$", "");
        if (trimmed.isEmpty() || trimmed.equals("0")) {
            return false;
        }
        return (trimmed.matches(POSITIVE_DECIMAL_REGEX));
    }

    public boolean checkPriceWithFourDecimal(String decimalPrice) {
        String fourDecimal = "^\\d+(\\.\\d{1,4})?$";
        if (StringUtil.isNullOrEmpty(decimalPrice)) {
            return false;
        }
        return Pattern.matches(fourDecimal, decimalPrice);
    }

    public boolean checkThisIsNumeric(String token) {
        int TOKEN_MAX_LENGTH = 98;
        String TOKEN_ALLOWED_CHARACTERS_REGEX = "^\\d+$";
        if (StringUtil.isNullOrEmpty(token)) {
            return false;
        }
        if (token.length() > TOKEN_MAX_LENGTH) {
            return false;
        }
        return Pattern.matches(TOKEN_ALLOWED_CHARACTERS_REGEX, token);
    }

    public boolean isPositiveWholeNumber(String input) {
        String trimmed = input.trim().replaceFirst("^0+(?!$)", "");
        if (trimmed.isEmpty() || trimmed.equals("0")) {
            return false;
        }
        return trimmed.matches(POSITIVE_WHOLENUMBER_REGEX);
    }

    public boolean checkPriceDecimal(String exch, String decimalPrice) {
        String twoDecimal  = "^\\d+\\.\\d{2}$";
        String fourDecimal = "^\\d+\\.\\d{4}$";
        if (StringUtil.isNullOrEmpty(decimalPrice)) {
            return false;
        }
        if (!StringUtil.isNullOrEmpty(decimalPrice)
                && (exch.equalsIgnoreCase("BSECURR") || exch.equalsIgnoreCase("NSECURR"))) {
            return Pattern.matches(fourDecimal, decimalPrice);
        }
        return Pattern.matches(twoDecimal, decimalPrice);
    }

    public boolean isAlphanumericOrNumeric(String str) {
        if (str == null) {
            return false;
        }
        return str.matches("^[a-zA-Z0-9]*$") || str.matches("^[0-9]*$");
    }

    public boolean isAlphaNumeric(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return false;
        }
        return str.matches("^[a-zA-Z0-9]*$");
    }

    public boolean isAlphanumericWithSpecialChars(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return false;
        }
        return str.matches("^[a-zA-Z0-9_-]*$");
    }

    public static long getExpiryInMilleSeconds() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        return cal.getTimeInMillis();
    }

    public String hashData(String data) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(data.getBytes());
        return Hex.toHexString(digest);
    }

    public String hashData1(String data) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(data.getBytes());
        for (int i = 1; i <= 999; i++) {
            digest = md.digest(digest);
        }
        return Hex.toHexString(digest);
    }

    public static void trustedManagement() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            } };
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HostnameVerifier allHostsValid = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) { return true; }
            };
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public int getExpiryInSeconds() {
        Calendar now      = Calendar.getInstance();
        Calendar midnight = Calendar.getInstance();
        midnight.add(Calendar.DATE, 1);
        midnight.set(Calendar.HOUR_OF_DAY, 0);
        midnight.set(Calendar.MINUTE, 0);
        midnight.set(Calendar.SECOND, 0);
        midnight.set(Calendar.MILLISECOND, 0);
        long millisecond = midnight.getTime().getTime() - now.getTime().getTime();
        return (int) (millisecond / 1000);
    }

    public byte[] compress(String data) throws IOException {
        if (data == null || data.isEmpty()) {
            return null;
        }
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(byteStream)) {
            gzip.write(data.getBytes("UTF-8"));
        }
        return byteStream.toByteArray();
    }
}
