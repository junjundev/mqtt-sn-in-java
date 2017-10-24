package mqttsn.gateway.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	
	public final static String STRING_ENCODING = "UTF-8";

	public static byte[] StringToUTF(String data) {
		try {
			byte[] utfBytes = data.getBytes(STRING_ENCODING);
			byte[] retArray = new byte[utfBytes.length+2];
				
			retArray[0] = new Integer(utfBytes.length/256).byteValue();
			retArray[1] = new Integer(utfBytes.length%256).byteValue();
				
			System.arraycopy( utfBytes, 0, retArray, 2, utfBytes.length);
			return (retArray);  
		} catch(UnsupportedEncodingException e) {
			LOG.error("Utils - Unsupported string encoding: "+STRING_ENCODING,e);
		}			
		return null;
	}
	
	
	/**
	 * @param data
	 * @param offset
	 * @return
	 */
	public static String UTFToString(byte[] data, int offset) {
		if (data == null)		
			return null;		
		int utflen = ((int) (data[0+offset] & 0xFF) << 8) + ((int) (data[1+offset] & 0xFF) << 0);
		if ((utflen + 2) > data.length)	
			return null;

		String retString = null;
		if (utflen > 0) {
			try {
				retString = new String( data, offset+2, utflen, STRING_ENCODING);
			} catch( UnsupportedEncodingException e) {
				LOG.error("Utils - Unsupported string encoding: "+STRING_ENCODING,e);
			}
		} else {
			retString = "";
		}
		
		return retString;
	}
	
		
	/**
	 * @param b
	 * @return
	 */
	public static String hexString(byte[] b) {
		String str = "";
		for(int i = 0; i < b.length; i++) {
			String t = "00" + Integer.toHexString(b[i]);
			if(i > 0) str += " ";
			str += t.substring(t.length() - 2);
		}
		return str;
	}

	
	/**
	 * @param data1
	 * @param data2
	 * @return
	 */
	public static byte[] concatArray(byte data1[],byte data2[]) {
		byte temp[] = new byte[data1.length + data2.length];
		System.arraycopy(data1, 0, temp, 0, data1.length);
		System.arraycopy(data2, 0, temp, data1.length, data2.length);
		return (temp);
	}
	
	
	/**
	 * @param data1
	 * @param off1
	 * @param len1
	 * @param data2
	 * @param off2
	 * @param len2
	 * @return
	 */
	public static byte[] concatArray(byte data1[],int off1, int len1, byte data2[], int off2, int len2) {
		byte temp[] = new byte[len1 + len2];
		System.arraycopy(data1, off1, temp, 0, len1);
		System.arraycopy(data2, off2, temp, len1, len2);
		return (temp);
	}

	/**
	 * @param data
	 * @param offset
	 * @param length
	 * @return
	 */
	public static byte[] SliceByteArray(byte data[], int offset, int length) {
		byte temp[] = new byte[length];
		System.arraycopy(data, offset, temp, 0, length);
		return (temp);
	}
}