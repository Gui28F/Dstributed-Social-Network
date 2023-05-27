package sd2223.trab1.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class FunctionSerializer implements Serializer<Function> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration required
    }

    @Override
    public byte[] serialize(String topic, Function function) {
        if (function == null) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(function);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Error serializing Function object", e);
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}
