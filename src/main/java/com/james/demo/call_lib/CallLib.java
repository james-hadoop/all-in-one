package com.james.demo.call_lib;

import com.james.lib.first.FirstLib;

public class CallLib {

    public static void main(String[] args) {
        String text = "plain text";
        String textMd5 = FirstLib.getMD5String(text);

        System.out.println("text:\n\t" + text);
        System.out.println("textMd5:\n\t" + textMd5);

    }

}
