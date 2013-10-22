package com.tensynchina.som.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;

public class CloudBinaryResponseParser extends ResponseParser {
	private SolrRequest request;
	public CloudBinaryResponseParser(SolrRequest request) {
		this.request = request;
	}

	@Override
	public String getWriterType() {
		String wt = request.getParams().get(CommonParams.WT);
		if(wt==null||wt.length()==0)wt="json";
		return wt;
	}

	@Override
	public NamedList<Object> processResponse(Reader reader) {
		throw new RuntimeException("Cannot handle character stream");
	}

	@Override
	public NamedList<Object> processResponse(InputStream body, String encode) {
		NamedList<Object> nl = new NamedList<Object>();
		try{
			nl.add("Stream", readStreamToBytes(body).array());
			nl.add("encode", encode);
		} catch (IOException e) {
	      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);

	    }
		return nl;
	}

	public static ByteBuffer readStreamToBytes(InputStream stream)
			throws IOException {
		byte[] bs = new byte[512];
		byte[] tmpbs = new byte[0];
		int ind = stream.read(bs);
		while (ind > 0) {
			byte[] tb = new byte[tmpbs.length];
			System.arraycopy(tmpbs, 0, tb, 0, tmpbs.length);
			tmpbs = new byte[tb.length + ind];
			System.arraycopy(tb, 0, tmpbs, 0, tb.length);
			System.arraycopy(bs, 0, tmpbs, tb.length, ind);
			ind = stream.read(bs);
		}
		ByteBuffer sb = ByteBuffer.wrap(tmpbs);
		return sb;
	}

}
