package co.elastic.es_flight.server;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public class SSLUtils {
    private static SSLContext yesCtx = null;

    /**
     * Returns an SSLContext that will accept any server certificate.
     * Use with great care and in limited situations, as it allows MITM attacks.
     */
    public static SSLContext yesSSLContext() {
        if (yesCtx == null) {

            X509TrustManager yesTm = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    // Accept anything
                }

                @Override
                public void  checkServerTrusted(X509Certificate[] certs, String authType) {
                    // Accept anything
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };

            try {
                SSLContext ctx = SSLContext.getInstance("SSL");
                ctx.init(null, new X509TrustManager[] { yesTm }, null);
                yesCtx = ctx;
            } catch (Exception e) {
                // An exception here means SSL is not supported, which is unlikely
                throw new RuntimeException(e);
            }
        }

        return yesCtx;
    }

    public static SSLSocketFactory yesSSLSocketFactory() {
        return yesSSLContext().getSocketFactory();
    }
}
