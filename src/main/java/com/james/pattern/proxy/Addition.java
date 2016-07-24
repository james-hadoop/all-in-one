package com.james.pattern.proxy;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Addition extends UnicastRemoteObject implements AdditionInterface {

	protected Addition() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public int add(int a, int b) throws RemoteException {
		// TODO Auto-generated method stub
		int result = a + b;
		return result;
	}

}
