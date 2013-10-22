package com.tensynchina.som.helper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class CloudPrintWriter extends PrintWriter {
	private final OutputStream os;
	private CloudPrintWriter(OutputStream out) {
		super(out);
		os = out;
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				try {
					os.close();
					System.out.println("OutputStream close success.");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		});
	}
	public CloudPrintWriter() {
		this(new ByteArrayOutputStream());
	}
	public String print(){
		this.flush();
		String rs = this.os.toString();
		((ByteArrayOutputStream) os).reset();
		return rs;
	}
}