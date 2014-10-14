package kafka;

import com.zcloud.snake.common.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * User: willstan
 * Date: 10/14/14
 * Time: 10:45 AM
 */
public class AvroTest {

    @Test
    public void testAvro() throws IOException {
        String strMessage = "qweqwe	-	asdasdasd	aa	true	asdasd	zzz	asdad	zxczxc	qweqwe		zxczxczxc	aa	zz	q	s	-	q	1233	s";
        GenericRecord gr = AvroUtils.generateGR("NewUser");
        Schema schema = AvroUtils.generateSchema("NewUser");
        List<Schema.Field> list =  schema.getFields();


        List<Schema.Field> fieldList = schema.getFields();
        String[] messages = strMessage.split("\t");
        System.out.println(messages.length);
        int i = 0;

        for (String msg : messages){
			if (msg != null && !msg.equals("-")) {
				Schema.Field field = fieldList.get(i);
				Schema.Type type = getFirstType(field);
				if (type == Schema.Type.LONG) {
					gr.put(field.name(), Long.parseLong(msg));
				}else{
					gr.put(field.name(), msg);
				}
			}
			i++;
        }

        System.out.println(gr);
    }

	private static Schema.Type getFirstType(Schema.Field field) {
		Schema.Type type = field.schema().getType();
		if (type == Schema.Type.UNION) {
			return field.schema().getTypes().get(0).getType();
		} else {
			return type;
		}
	}
}
