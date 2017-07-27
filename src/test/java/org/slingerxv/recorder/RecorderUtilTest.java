package org.slingerxv.recorder;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RecorderUtilTest {

	@Test
	public void getLogTableName() {
		String logTableName = RecorderUtil.getLogTableName(new UserLog(), 1501138771000L);
		String logTableName1 = RecorderUtil.getLogTableName(new UserLog2(), 1501138771000L);
		String logTableName2 = RecorderUtil.getLogTableName(new UserLog3(), 1501138771000L);
		String logTableName3 = RecorderUtil.getLogTableName(new UserLog4(), 1501138771000L);
		assertEquals("userlog20170727", logTableName);
		assertEquals("userlog2201707", logTableName1);
		assertEquals("userlog32017", logTableName2);
		assertEquals("userlog4", logTableName3);
	}

}
