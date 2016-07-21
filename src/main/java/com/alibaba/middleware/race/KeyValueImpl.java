package com.alibaba.middleware.race;

import java.util.HashMap;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.TypeException;

/**
 * 实现KeyValue接口
 * @author hankwing
 *
 */
public class KeyValueImpl implements KeyValue{

	String key = null;
	String rawValue = null;
	
	public KeyValueImpl( String key, String rawValue) {
		this.key = key;
		this.rawValue = rawValue;
	}
	
	@Override
	public String key() {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public String valueAsString() {
		// TODO Auto-generated method stub
		return rawValue;
	}

	@Override
	public long valueAsLong() throws TypeException {
		// TODO Auto-generated method stub
		try {
			return Long.parseLong(rawValue);
		} catch (NumberFormatException e) {
			System.out.println("long failed!" + rawValue);
			throw new TypeException();
		}
	}

	@Override
	public double valueAsDouble() throws TypeException {
		// TODO Auto-generated method stub
		try {
			return Double.parseDouble(rawValue);
		} catch (NumberFormatException e) {
			throw new TypeException();
		}
	}

	@Override
	public boolean valueAsBoolean() throws TypeException {
		// TODO Auto-generated method stub
		if (this.rawValue.equals(RaceConfig.booleanTrueValue)) {
			return true;
		}
		if (this.rawValue.equals(RaceConfig.booleanFalseValue)) {
			return false;
		}
		throw new TypeException();
	}
	
	@Override
	public String toString() {
		return "[" + this.key + "]:" + this.rawValue;
	}

}
