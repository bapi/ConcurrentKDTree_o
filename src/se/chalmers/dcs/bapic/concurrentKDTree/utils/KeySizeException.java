package se.chalmers.dcs.bapic.concurrentKDTree.utils;

/**
 * KeySizeException is thrown when a KDTree method is invoked on a key whose
 * size (array length) mismatches the one used in the that KDTree's constructor.
 *
 */
public class KeySizeException extends DimensionLimitException {

    protected KeySizeException() {
        super("Key size mismatch");
    }

    /*
     *   The serialization runtime associates with each serializable class a version number,
     *   called a serialVersionUID, which is used during deserialization to verify that the 
     *   sender and receiver of a serialized object have loaded classes for that object that
     *   are compatible with respect to serialization. 
     */
    public static final long serialVersionUID = 40L;

}
