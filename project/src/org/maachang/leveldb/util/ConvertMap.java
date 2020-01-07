package org.maachang.leveldb.util;

/**
 * Get変換機能を持つBlankMap.
 */
public interface ConvertMap extends BlankMap, ConvertGet<Object> {
	@Override
	default Object getOriginal(Object o) {
		return get(o);
	}
}
