import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;

public class Main {
    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Usage: Avro2Json avrofile");
            return;
        }

        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(args[0]), datumReader);
            Schema schema = dataFileReader.getSchema();
            System.out.println("Avro schema:");
            System.out.println(schema);

            System.out.println("\nRecords:");
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                System.out.println(record);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}
