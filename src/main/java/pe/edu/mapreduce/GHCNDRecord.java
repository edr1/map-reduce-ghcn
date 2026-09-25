package pe.edu.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.OptionalDouble;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Parser del formato CSV de GHCN-Daily access/. */
public final class GHCNDRecord {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int PRCP = 6;
    private static final int PRCP_ATTRIBUTES = 7;
    private static final int TMAX = 8;
    private static final int TMAX_ATTRIBUTES = 9;

    private final CSVRecord row;
    private final LocalDate date;

    private GHCNDRecord(CSVRecord row, LocalDate date) {
        this.row = row;
        this.date = date;
    }

    public static boolean isHeader(String line) {
        String normalized = line == null ? "" : line.replace("\"", "").trim();
        return normalized.startsWith("STATION,DATE,");
    }

    public static GHCNDRecord parse(String line) throws IOException {
        try (CSVParser parser = CSVFormat.DEFAULT.parse(new StringReader(line))) {
            Iterator<CSVRecord> iterator = parser.iterator();
            if (!iterator.hasNext()) {
                throw new IOException("Registro CSV vacío");
            }
            CSVRecord record = iterator.next();
            if (record.size() <= TMAX_ATTRIBUTES) {
                throw new IOException("Registro con columnas insuficientes: " + record.size());
            }
            try {
                return new GHCNDRecord(record, LocalDate.parse(record.get(DATE).trim()));
            } catch (DateTimeParseException e) {
                throw new IOException("Fecha inválida", e);
            }
        }
    }

    public String station() { return row.get(STATION).trim(); }
    public LocalDate date() { return date; }

    public Measurement tmax() {
        return measurement(TMAX, TMAX_ATTRIBUTES);
    }

    public Measurement precipitation() {
        return measurement(PRCP, PRCP_ATTRIBUTES);
    }

    private Measurement measurement(int valueIndex, int attributesIndex) {
        String raw = row.get(valueIndex).trim();
        if (raw.isEmpty() || raw.matches("-?9{2,}")) {
            return Measurement.missing();
        }

        String attributes = row.get(attributesIndex);
        String[] flags = attributes.split(",", -1);
        String qualityFlag = flags.length > 1 ? flags[1].trim() : "";
        if (!qualityFlag.isEmpty()) {
            return Measurement.rejected(qualityFlag);
        }

        try {
            // Los archivos access/ proporcionados usan décimas: 276 = 27,6 °C;
            // PRCP=15 representa 1,5 mm.
            return Measurement.valid(Double.parseDouble(raw) / 10.0);
        } catch (NumberFormatException e) {
            return Measurement.missing();
        }
    }

    public static final class Measurement {
        private final OptionalDouble value;
        private final String qualityFlag;

        private Measurement(OptionalDouble value, String qualityFlag) {
            this.value = value;
            this.qualityFlag = qualityFlag;
        }

        static Measurement valid(double value) {
            return new Measurement(OptionalDouble.of(value), "");
        }

        static Measurement missing() {
            return new Measurement(OptionalDouble.empty(), "");
        }

        static Measurement rejected(String flag) {
            return new Measurement(OptionalDouble.empty(), flag);
        }

        public boolean isValid() { return value.isPresent(); }
        public boolean isRejectedByQuality() { return !qualityFlag.isEmpty(); }
        public double value() { return value.orElseThrow(); }
    }
}
