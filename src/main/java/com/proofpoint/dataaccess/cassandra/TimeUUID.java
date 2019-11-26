package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.utils.UUIDs;
import com.proofpoint.units.Duration;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

/**
 * Cassandra TIMEUUID type, to differentiate this type from UUID in method signatures
 */
public final class TimeUUID implements Comparable<TimeUUID>, Comparator<TimeUUID>, Serializable
{
    private final UUID uuid;

    public TimeUUID(UUID uuid)
    {
        if (uuid == null || uuid.version() != 1) {
            throw new UnsupportedOperationException("Not a time-based UUID");
        }
        this.uuid = uuid;
    }

    public UUID getUUID()
    {
        return uuid;
    }

    public long timestamp()
    {
        return getTimeFromUUID(uuid);
    }

    public Date getDate()
    {
        return new Date(timestamp());
    }

    // This method comes from Hector's TimeUUIDUtils class:
    // https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/cassandra/utils/TimeUUIDUtils.java

    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    public static long getTimeFromUUID(UUID uuid)
    {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    @Override
    public int compareTo(TimeUUID o)
    {
        long timeDiff = this.timestamp() - o.timestamp();
        if (timeDiff == 0) {
            return uuid.compareTo(o.uuid);
        }
        else {
            return (timeDiff < 0) ? -1 : 1;
        }
    }


    @Override
    public int hashCode()
    {
        return uuid.hashCode();
    }

    @Override
    public int compare(TimeUUID o1, TimeUUID o2)
    {
        return o1.compareTo(o2);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof TimeUUID)) {
            return false;
        }
        return this.uuid.equals(((TimeUUID) o).getUUID());
    }

    public static TimeUUID now()
    {
        return new TimeUUID(UUIDs.timeBased());
    }

    public static TimeUUID fromString(String s)
    {
        return new TimeUUID(UUID.fromString(s));
    }

    public static TimeUUID getMinTimeUUID(long time)
    {
        return new TimeUUID(new UUID(mostSignificantBits(time, 1), 0));
    }

    public static TimeUUID getMaxTimeUUID(long time)
    {
        return new TimeUUID(new UUID(mostSignificantBits(time, 1), ~0));
    }

    private static long mostSignificantBits(long timestamp, int version)
    {

        // The 60 bit timestamp value is constructed from the time_low, time_mid, and time_hi fields of UUID.
        // The resulting timestamp is measured in 100-nanosecond units since midnight, October 15, 1582 UTC.
        // 0xFFFFFFFF00000000 time_low
        // 0x00000000FFFF0000 time_mid
        // 0x000000000000F000 version
        // 0x0000000000000FFF time_hi

        long uuidEpochTimeIn100Nanos = (timestamp * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
        return
                (uuidEpochTimeIn100Nanos << 32) | // time_low
                        ((uuidEpochTimeIn100Nanos & 0xFFFF00000000L) >> 16) | // time_mid
                        (version & 0xF) << 12 | // version
                        ((uuidEpochTimeIn100Nanos >> 48) & 0x0FFFL); // time_hi
    }

    public String toString()
    {
        return uuid.toString();
    }

    public String toDebugString()
    {
        return uuid.toString() + "(" + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(getDate()) + ")";
    }

    public boolean isWithin(Duration retentionPeriod)
    {
        long now = System.currentTimeMillis();
        return this.timestamp() > (now - retentionPeriod.toMillis()) && this.timestamp() < now;
    }
}
