package hu.blackbelt.judo.operation.utils;

public class Kleene {

    public static Boolean or(Boolean left, Boolean right) {
        if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
            return Boolean.TRUE;
        } else if (left == null || right == null) {
            return null;
        } else {
            return Boolean.FALSE;
        }
    }

    public static Boolean and(Boolean left, Boolean right) {
        if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
            return Boolean.FALSE;
        } else if (left == null || right == null) {
            return null;
        } else {
            return Boolean.TRUE;
        }
    }

    public static Boolean xor(Boolean left, Boolean right) {
        if (left == null || right == null) {
            return null;
        } else if (left.equals(right)) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    public static Boolean implies(Boolean left, Boolean right) {
        if (Boolean.TRUE.equals(left) && Boolean.FALSE.equals(right)) {
            return Boolean.FALSE;
        } else if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
            return Boolean.TRUE;
        } else {
            return null;
        }
    }

    public static Boolean not(Boolean operand) {
        return operand == null ? null : !operand;
    }
}
