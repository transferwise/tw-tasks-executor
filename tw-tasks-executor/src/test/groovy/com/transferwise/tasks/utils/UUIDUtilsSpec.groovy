package com.transferwise.tasks.utils

import org.apache.commons.lang3.RandomUtils
import spock.lang.Specification

class UUIDUtilsSpec extends Specification {
    def "converting from UUID and back to bytes end with the same result"() {
        given:
            UUID uuid = UUID.randomUUID();
        when:
            byte[] bytes = UUIDUtils.toBytes(uuid)
            UUID uuid1 = UUIDUtils.toUUID(bytes)
        then:
            uuid == uuid1
    }

    def "converting from bytes and back to UUID end with the same result"() {
        given:
            byte[] bytes = RandomUtils.nextBytes(16)
        when:
            UUID uuid = UUIDUtils.toUUID(bytes)
            byte[] bytes1 = UUIDUtils.toBytes(uuid);
        then:
            bytes == bytes1;
    }
}
