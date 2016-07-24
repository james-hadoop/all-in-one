package com.james.pattern.command;

public class Main {
    public static void main(String[] args) {
        RemoteControl rc = new RemoteControl();

        Light light = new Light("KitchenLight");

        LightOnCommand lOnC = new LightOnCommand(light);
        LightOffCommand lOffC = new LightOffCommand(light);

        rc.setCommand(0, lOnC, lOffC);

        rc.onButtonWasPushed(0);
        rc.offButtonWasPushed(0);
    }
}
