package be.wegenenverkeer.atomium.api;

/**
 * Created by Karel Maesen, Geovise BVBA on 15/11/16.
 */
public class AtomiumEncodeException extends RuntimeException {
    public AtomiumEncodeException(String message, Throwable t) {
        super(message, t);
    }
}
