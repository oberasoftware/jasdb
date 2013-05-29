package nl.renarj.jasdb.service;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Renze de Vries
 */
public class MachineGuidGenerator implements IdGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(MachineGuidGenerator.class);

    private static final long machineId = generateMachineId();
    private static final AtomicLong startId = new AtomicLong(System.currentTimeMillis());

    @Override
    public String generateNewId() throws JasDBStorageException {
        long sequentialId = startId.incrementAndGet();
        return new UUID(machineId, sequentialId).toString();
    }

    private static long generateMachineId() {
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while(networkInterfaceEnumeration.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaceEnumeration.nextElement();
                if(!networkInterface.isLoopback() && networkInterface.getHardwareAddress() != null) {
                    byte[] hardwareAddress = networkInterface.getHardwareAddress();

                    long machineId = hardwareAddress[0] & 0xFF;
                    for (int i = 1; i < 6; ++i) {
                        machineId = (machineId << 8) | (hardwareAddress[i] & 0xFF);
                    }

                    return machineId;
                }
            }
        } catch(SocketException e) {
            LOG.error("Unable to determine machineId", e);
        }
        LOG.info("Could not determine machineId, using system time for machineId");
        return System.nanoTime();
    }
}
