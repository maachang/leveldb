package org.maachang.leveldb.util;

import java.util.Map;

/**
 * Get変換機能を持つBlankMap.
 */
public interface ConvertMap extends Map<Object, Object>, ConvertGet<Object> {
	@Override
	default Object getOriginal(Object o) {
		return get(o);
	}
}
