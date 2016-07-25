package com.alibaba.middleware.tools;

import java.util.Arrays;

public class BytesKey {
	private final byte[] key;

    public BytesKey(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BytesKey cacheKey = (BytesKey) o;
        return Arrays.equals(key, cacheKey.key);
    }

    @Override
    public int hashCode() {
        return key != null ? Arrays.hashCode(key) : 0;
    }
}
