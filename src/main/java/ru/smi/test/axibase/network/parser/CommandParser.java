package ru.smi.test.axibase.network.parser;

import com.axibase.tsd.model.data.series.InterpolateType;
import com.axibase.tsd.model.data.series.Sample;
import com.axibase.tsd.model.meta.DataType;
import com.axibase.tsd.network.InsertCommand;
import com.axibase.tsd.network.MessageInsertCommand;
import com.axibase.tsd.network.MultipleInsertCommand;
import com.axibase.tsd.network.PlainCommand;
import com.axibase.tsd.network.PropertyInsertCommand;
import com.axibase.tsd.network.SimpleCommand;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.text.ParseException;
import java.util.*;

public class CommandParser implements Parser {

    private static final Logger log = LoggerFactory.getLogger(CommandParser.class);
    private static final FastDateFormat FDF_ISO = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"));

    private static final String SERIES = "series";
    private static final String PROPERTY = "property";
    private static final String MESSAGE = "message";
    private static final String METRIC = "metric";
    private static final String ENTITY = "entity";

    @Override
    public PlainCommand parse(String command) {
        try {
            String[] parts = splitQuoted(command, " ", false, false);
            switch (parts[0]) {
            case SERIES: {
                return parseSeries(parts);
            }
            case PROPERTY: {
                return parseProperty(parts);
            }
            case MESSAGE: {
                return parseMessage(parts);
            }
            case METRIC: {
                return new SimpleCommand(command);
            }
            case ENTITY: {
                return new SimpleCommand(command);
            }
            }
        } catch (Throwable e) {
            log.error("Could not parse command: {}", command, e);
        }
        log.warn("Unknown command: {}", command);
        return new SimpleCommand(command);
    }

    private PlainCommand parseProperty(String[] parts) throws ParseException {
        // property e:<entity> s:<timestamp> t:<type> k:<key-1>=<value-1>
        // k:<key-2>=<value-2> v:<name-1>=<value-1> v:<name-2>=<value-2>
        MultiValueMap<String, String> prefixToValues = parseParts(PROPERTY, parts);
        return new PropertyInsertCommand(resolveEntity(prefixToValues), prefixToValues.getFirst("t"), resolveTime(prefixToValues),
                resolveKeyValues(prefixToValues, "k"), resolveKeyValues(prefixToValues, "v"));
    }

    private PlainCommand parseSeries(String[] parts) throws ParseException {
        // series e:server001 m:disk_used_percent=20.5 m:disk_size_mb=10240
        // x:disk_size="10 GB" t:mount_point=/ t:disk_name=/sda1
        MultiValueMap<String, String> prefixToValues = parseParts(SERIES, parts);

        List<String> numericValueList = prefixToValues.get("m");
        List<String> textValueList = prefixToValues.get("x");

        final int mListSize = numericValueList == null ? 0 : numericValueList.size();
        final int textListSize = textValueList == null ? 0 : textValueList.size();
        if (mListSize + textListSize == 1) {
            return parseSingleSeries(numericValueList, textValueList, prefixToValues, mListSize == 1);
        } else {
            return parseMultipleSeries(numericValueList, textValueList, prefixToValues);
        }
    }

    private InsertCommand parseSingleSeries(List<String> mavList, List<String> textValuesList, MultiValueMap<String, String> prefixToValues,
            boolean isNumeric) throws ParseException {
        Double numberValue;
        String textValue = null;
        String[] metricAndValue;
        if (isNumeric) {
            metricAndValue = parseMetricAndValue(mavList);
            numberValue = parseNumber(metricAndValue[1]).doubleValue();
        } else {
            numberValue = Double.NaN;
            metricAndValue = parseMetricAndValue(textValuesList);
            if (metricAndValue.length == 2) {
                textValue = metricAndValue[1];
            }
        }
        return new InsertCommand(resolveEntity(prefixToValues), metricAndValue[0], new Sample(resolveTime(prefixToValues), numberValue, textValue),
                resolveKeyValues(prefixToValues, "t"));
    }

    private static String[] parseMetricAndValue(List<String> mavList) {
        String mavPart = mavList.get(0);
        String[] metricAndValue = StringUtils.split(mavPart, "=");
        if (metricAndValue.length != 2) {
            log.error("Illegal metric name and value: {}", mavPart);
        }
        return metricAndValue;
    }

    @SuppressWarnings("unchecked")
    private MultipleInsertCommand parseMultipleSeries(List<String> mavList, List<String> textValuesList, MultiValueMap<String, String> prefixToValues)
            throws ParseException {
        Map<String, Double> numericValues = (Map<String, Double>) parseSeriesValues(mavList, true);
        Map<String, String> textValues = (Map<String, String>) parseSeriesValues(textValuesList, false);
        return new MultipleInsertCommand(resolveEntity(prefixToValues), resolveTime(prefixToValues), resolveKeyValues(prefixToValues, "t"),
                numericValues, textValues);
    }

    private Map<?, ?> parseSeriesValues(List<String> metricValueList, boolean parseNumber) throws ParseException {
        if (metricValueList == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> values = new HashMap<>();
        for (String mavPart : metricValueList) {
            String[] metricAndValue = StringUtils.split(mavPart, "=");
            if (metricAndValue.length != 2) {
                log.error("Illegal metric name and value: {}", mavPart);
            } else {
                final Object value = parseNumber ? parseNumber(metricAndValue[1]).doubleValue() : metricAndValue[1];
                values.put(metricAndValue[0], value);
            }
        }
        return values;
    }

    private MessageInsertCommand parseMessage(String[] parts) throws ParseException {
        MultiValueMap<String, String> prefixToValues = parseParts(MESSAGE, parts);
        return new MessageInsertCommand(resolveEntity(prefixToValues), resolveTime(prefixToValues), resolveKeyValues(prefixToValues, "t"),
                prefixToValues.getFirst("m"));
    }

    public DataType resolveDataType(String value) {
        return value == null ? null : DataType.valueOf(value.toUpperCase());
    }

    public String resolveEntity(MultiValueMap<String, String> prefixToValues) {
        return prefixToValues.getFirst("e");
    }

    public long resolveTime(MultiValueMap<String, String> prefixToValues) throws ParseException {
        if (prefixToValues.containsKey("ms")) {
            String ms = prefixToValues.getFirst("ms");
            return Long.parseLong(ms);
        } else if (prefixToValues.containsKey("d")) {
            String d = prefixToValues.getFirst("d");
            return FDF_ISO.parse(d).getTime();
        } else {
            return System.currentTimeMillis();
        }

    }

    private MultiValueMap<String, String> parseParts(String putName, String[] parts) {
        MultiValueMap<String, String> prefixToValues = new LinkedMultiValueMap<String, String>();
        for (String part : parts) {
            if (!putName.equals(part)) {
                String[] prefixAndValue = StringUtils.split(part, ":", 2);
                if (prefixAndValue.length != 2) {
                    if (prefixAndValue.length == 1 && ((MESSAGE.equals(putName) && "m".equals(prefixAndValue[0]))
                            || ((ENTITY.equals(putName) || METRIC.equals(putName)) && "l".equals(prefixAndValue[0])))) {
                        prefixToValues.add(prefixAndValue[0], "");
                    } else {
                        throw new IllegalArgumentException("Illegal part: " + part);
                    }
                } else {
                    prefixToValues.add(prefixAndValue[0], prefixAndValue[1]);
                }
            }
        }
        return prefixToValues;
    }

    private Map<String, String> resolveKeyValues(MultiValueMap<String, String> prefixToValues, String prefix) {
        List<String> parts = prefixToValues.get(prefix);

        if (CollectionUtils.isEmpty(parts)) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new HashMap<String, String>();
        for (String part : parts) {
            String[] keyAndValue = StringUtils.split(part, "=");

            if (keyAndValue.length > 2) {
                String value = part.substring(part.indexOf("=") + 1);
                map.put(keyAndValue[0], value);
            } else if (keyAndValue.length == 1) {
                map.put(keyAndValue[0], "");
            } else {
                map.put(keyAndValue[0], keyAndValue[1]);
            }
        }

        return map;
    }

    // from ATSD, com.axibase.tsd.service.Util
    public static String[] splitQuoted(String value, String delimiter, boolean preserveQuotes) {
        return splitQuoted(value, delimiter, preserveQuotes, true);
    }

    public static String[] splitQuoted(String value, String delimiter, boolean preserveQuotes, boolean preserveBlank) {
        List<String> result = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(value, delimiter + "\"", true);
        boolean quoted = false;
        StringBuilder sb = new StringBuilder();
        String prevToken = null;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            boolean append = true;
            if ("\"".equals(token)) {
                if (quoted) {
                    quoted = false;
                    append = preserveQuotes;
                } else if ("\"".equals(prevToken)) {
                    quoted = true;
                } else {
                    quoted = true;
                    append = preserveQuotes;
                }
            }

            if (!quoted && String.valueOf(delimiter).equals(token)) {
                if (sb.length() > 0) {
                    result.add(sb.toString());
                    sb = new StringBuilder();
                }
            } else if (append) {
                sb.append(token);
            }

            prevToken = token;
        }
        if (sb.length() > 0) {
            String part = sb.toString();
            if (preserveBlank || StringUtils.isNoneBlank(part)) {
                result.add(part);
            }
        }
        return result.toArray(new String[result.size()]);
    }

    public InterpolateType resolveInterpolateType(String value) {
        return value == null ? null : InterpolateType.valueOf(value.toUpperCase());
    }

    public TimeZone resolveTimeZone(String value) {
        return value == null ? null : TimeZone.getTimeZone(value);
    }

    private CommandParser() {
    }

    // thread-safe statically initialization
    public static CommandParser getInstance() {
        return InstanceHolder.instance;
    }

    private static class InstanceHolder {
        private static final CommandParser instance = new CommandParser();
    }

    public static Pair<String, String> getField(String part) {
        int index = part.indexOf(':');
        if (index < 1) {
            throw new IllegalArgumentException("Invalid part: " + part);
        }
        return new ImmutablePair<String, String>(part.substring(0, index), part.substring(index + 1));
    }

    private static Number parseNumber(String number) {
        if ("NaN".equals(number)) {
            return Double.NaN;
        }
        return NumberUtils.createNumber(number);
    }

}
