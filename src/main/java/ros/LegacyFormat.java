package ros;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A class for easy conversion to the legacy java_rosbridge {@link RosListenDelegate#accept(com.fasterxml.jackson.databind.JsonNode, String)}
 * message format that presented the JSON data
 * in a {@link java.util.Map} from {@link java.lang.String} to {@link java.lang.Object} instances
 * in which the values were ether primitives, {@link java.util.Map} objects themselves, or {@link java.util.List}
 * objects.
 */
public final class LegacyFormat {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    /**
     * A method for easy conversion to the legacy java_rosbridge {@link RosListenDelegate#accept(com.fasterxml.jackson.databind.JsonNode, String)}
     * message format that presented the JSON data
     * in a {@link java.util.Map} from {@link java.lang.String} to {@link java.lang.Object} instances
     * in which the values were ether primitives, {@link java.util.Map} objects themselves, or {@link java.util.List}
     * objects.
     *
     * @param jsonString the source JSON string message that was received
     * @return a {@link java.util.Map} data structure of the JSON data.
     */
    public static final Map<String, Object> legacyFormat(String jsonString) {

        final JsonFactory jsonFactory = new JsonFactory();

        try {
            final ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
            final TypeReference<Map<String, Object>> listTypeRef = new TypeReference<>() {
            };
            final Map<String, Object> messageData = objectMapper.readValue(jsonString, listTypeRef);
            return messageData;
        } catch (final JsonParseException jsonParseException) {
            LOGGER.error(ExceptionUtils.getStackTrace(jsonParseException));
            throw new RuntimeException(jsonParseException);
        } catch (IOException ioException) {
            LOGGER.error(ExceptionUtils.getStackTrace(ioException));
            throw new RuntimeException(ioException);
        }


    }
}

