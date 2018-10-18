package org.corfudb.security.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

@Slf4j
public class SslContextConstructor {

    private SslContextConstructor() {
        //prevent instatiation
    }

    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param isServer  Server or client
     * @param sslConfig ssl configuration
     * @return SslContext object.
     * @throws SSLException Wrapper exception for any issue reading the key/trust store.
     */
    public static SslContext constructSslContext(boolean isServer, SslConfig sslConfig) throws SSLException {
        log.info("Construct ssl context based on the following information: {}", sslConfig);

        KeyManagerFactory kmf = createKeyManagerFactory(sslConfig.getKeyStore(), sslConfig.getKsPasswordFile());
        ReloadableTrustManagerFactory tmf = new ReloadableTrustManagerFactory(
                sslConfig.getTrustStore(),
                sslConfig.getTsPasswordFile()
        );

        if (isServer) {
            return SslContextBuilder
                    .forServer(kmf)
                    .sslProvider(sslConfig.getProvider())
                    .trustManager(tmf)
                    .build();
        } else {
            return SslContextBuilder
                    .forClient()
                    .sslProvider(sslConfig.getProvider())
                    .keyManager(kmf)
                    .trustManager(tmf)
                    .build();
        }
    }

    private static KeyManagerFactory createKeyManagerFactory(String keyStorePath,
                                                             String ksPasswordFile) throws SSLException {
        String keyStorePassword = TlsUtils.getKeyStorePassword(ksPasswordFile);
        KeyStore keyStore = TlsUtils.openKeyStore(keyStorePath, keyStorePassword);

        KeyManagerFactory kmf;
        try {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyStorePassword.toCharArray());
            return kmf;
        } catch (UnrecoverableKeyException e) {
            String errorMessage = "Unrecoverable key in key store " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Can not create key manager factory with default algorithm "
                    + KeyManagerFactory.getDefaultAlgorithm() + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Can not initialize key manager factory from " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        }
    }

    @Builder
    @Getter
    @Value
    @ToString
    public static class SslConfig {
        @Default
        SslProvider provider = SslProvider.OPENSSL;
        /**
         * A path to the key store.
         */
        @NonNull
        @Default
        String keyStore;

        /**
         * A file containing the password for the key store.
         */
        String ksPasswordFile;

        /**
         * A path to the trust store.
         */
        @NonNull
        String trustStore;

        /**
         * A path containing the password for the trust store.
         */
        String tsPasswordFile;
    }

    @Builder
    @Value
    public static class SaslConfig {
        /**
         * True, if SASL plain text authentication is enabled.
         */
        @Default
        boolean saslPlainTextEnabled = false;

        /**
         * A path containing the username file for SASL.
         */
        String usernameFile;

        /**
         * A path containing the password file for SASL.
         */
        String passwordFile;
    }
}
