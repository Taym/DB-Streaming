import io.vavr.control.Try;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class PostgreSqlCopyUtil {
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    /**
     * Inserts the provided stream of data(dataStream) into the database table that is represented by
     * the Class<T> object(clazz).
     * @param clazz the Class object of the class that represents the database table.
     * @param dataStream the stream of data that will be inserted into the database table.
     * @param dataSource the DataSource that is used to get a connection to the targeted database.
     * @return the total number of rows inserted wrapped in Try<> in case an exception occurred.
     */
    public static <T> Try<Long> writeToDatabase(final Class<T> clazz, final Stream<T> dataStream,final DataSource dataSource) {
        try (PipedInputStream is = new PipedInputStream()) {
            try (PipedOutputStream os = new PipedOutputStream(is)) {
                try (OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
                    try (BufferedWriter bw = new BufferedWriter(osw)) {
                        final BaseConnection con = (BaseConnection) DataSourceUtils.getConnection(dataSource);
                        final CopyManager copyManager = new CopyManager(con);
                        try (CSVPrinter printer = new CSVPrinter(bw, CSVFormat.DEFAULT)) {
                            final Supplier<Try<Void>> supp = () -> Try.run(() -> {
                                Optional<RuntimeException> possibleSuppressedException = Optional.empty();
                                try{
                                    final Stream<List<String>> convertedToCsv = dataStream.map(obj -> CsvMappingUtil.mapObjectToCsvRecord(obj, clazz));
                                    convertedToCsv.forEach(csvRecord -> printCsvRecord(printer, csvRecord));
                                }
                                catch (RuntimeException e){
                                    possibleSuppressedException = Optional.of(e);
                                    throw new RuntimeException(e);
                                }
                                finally {
                                    closeResource(printer,possibleSuppressedException);
                                }
                            });
                            final CompletableFuture<Try<Void>> generateResult = CompletableFuture.supplyAsync(supp, threadPool);

                            final String columns = Stream
                                    .of(clazz.getDeclaredFields())
                                    .map(field -> field.getName())
                                    .collect(Collectors.joining(",", "(", ")"));
                            final String copyCommand = "COPY " + clazz.getSimpleName() + columns + "FROM STDIN (DELIMITER ',', NULL 'NULL', FORMAT 'csv')";
                            final Try<Long> rowsWritten = Try.of(() -> copyManager.copyIn(copyCommand, is));

                            final Try<Void> writerThreadResult = generateResult.join();
                            if (writerThreadResult.isFailure()) {
                                return writerThreadResult.map(ignore -> null);
                            } else {
                                return rowsWritten;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            return Try.failure(e);
        }
    }

    private static void printCsvRecord(final CSVPrinter csvPrinter, final List<String> record) {
        try {
            csvPrinter.printRecord(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeResource(final Closeable resource, final Optional<RuntimeException> possibleSuppressedException){
        try {
            resource.close();
        } catch (Exception e) {
            throw possibleSuppressedException.orElse(new RuntimeException(e));
        }
    }
}
