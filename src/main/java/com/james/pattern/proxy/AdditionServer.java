package com.james.pattern.proxy;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;

public class AdditionServer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.setSecurityManager(new RMISecurityManager());

			Addition Hello = new Addition();
			
			LocateRegistry.createRegistry(1099);
			Naming.rebind("RemoteAdd", Hello);

			System.out.println("Addition Server is ready.");
		} catch (Exception e) {
			System.out.println("Addition Server failed: " + e);
		}
	}
}
