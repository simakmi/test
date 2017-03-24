package ru.smi.test.axibase.network.parser;

import com.axibase.tsd.network.PlainCommand;

public interface Parser {

    public PlainCommand parse(String input);

}
