package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JsonParser extends UDF {
    public String evaluate(String line) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            MovieRateBean movieRateBean = mapper.readValue(line, MovieRateBean.class);
            return movieRateBean.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
