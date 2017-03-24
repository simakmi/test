package ru.smi.test.axibase.network;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.axibase.tsd.network.PlainCommand;

import ru.smi.test.axibase.network.parser.CommandParser;

public class PlainCommandDeserializer implements Deserializer<PlainCommand> {

    private static final String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub
    }

    @Override
    public PlainCommand deserialize(String topic, byte[] data) {
        String command;
        try {
            if (data == null) {
                return null;
            }
            command = new String(data, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + encoding);
        }
        return CommandParser.getInstance().parse(command);
    }

    @Override
    public void close() {
        // nothing to do
    }

}
