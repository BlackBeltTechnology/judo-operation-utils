package hu.blackbelt.judo.operation.utils;

/*-
 * #%L
 * Judo Operation Utils
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

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
