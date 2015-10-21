package com.yiban.datacenter.finalversion;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class HbaseServer {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		backprocess();
	}
	
	public static void backprocess(){
		try {
			ServerSocket ss=new ServerSocket(11111);
			while(true){
				Socket s=ss.accept();
				
				Thread deal=new Thread(new DealUserThread(s));
				deal.setDaemon(true);
				deal.start();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
