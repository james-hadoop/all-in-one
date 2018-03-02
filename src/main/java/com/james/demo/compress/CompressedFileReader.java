package com.james.demo.compress;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.james.json.json_schema_validation.JsonSchemaValidator;
import com.james.json.json_schema_validation.ValidationResult;

public class CompressedFileReader {
    public static void main(String[] args) throws Exception {
        processGzipFile("a.gz", "output.txt", false);
    }

    /**
     * processGzipFile
     * 
     * @param path
     * @param outputPath
     * @param flagFilter
     * @throws Exception
     */
    public static void processGzipFile(String path, String outputPath, boolean flagFilter) throws Exception {
        if (null == path || path.isEmpty()) {
            return;
        }

        List<String> lines = readGzipFile(path);

        List<String> validatedLines = validateJsonSchema(lines, flagFilter);

        saveLines(validatedLines, outputPath);
    }

    /**
     * processGzipInputStream
     * 
     * @param inputStream
     * @param outputPath
     * @param flagFilter
     * @throws Exception
     */
    public static void processGzipInputStream(InputStream inputStream, String outputPath, boolean flagFilter)
            throws Exception {
        if (null == inputStream) {
            return;
        }

        List<String> lines = readGzipInputStream(inputStream);

        List<String> validatedLines = validateJsonSchema(lines, flagFilter);

        saveLines(validatedLines, outputPath);
    }

    /**
     * readGzipInputStream
     * 
     * @param inputStream
     * @return
     * @throws Exception
     */
    public static List<String> readGzipInputStream(InputStream inputStream) throws Exception {
        if (null == inputStream) {
            return null;
        }

        BufferedReader br = null;
        try {
            List<String> lines = new ArrayList<String>();
            GZIPInputStream gzin = new GZIPInputStream(inputStream);
            BufferedInputStream bis = new BufferedInputStream(gzin);
            br = new BufferedReader(new InputStreamReader(bis));

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                lines.add(line);
            }

            return lines;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            br.close();
        }
    }

    /**
     * readGzipFile
     * 
     * @param path
     * @return
     * @throws Exception
     */
    public static List<String> readGzipFile(String path) throws Exception {
        if (null == path || path.isEmpty()) {
            return null;
        }

        BufferedReader br = null;
        try {
            List<String> lines = new ArrayList<String>();
            GZIPInputStream gzin = new GZIPInputStream(new FileInputStream(path));
            BufferedInputStream bis = new BufferedInputStream(gzin);
            br = new BufferedReader(new InputStreamReader(bis));

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                lines.add(line);
            }

            return lines;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            br.close();
        }
    }

    /**
     * validateJsonSchema
     * 
     * @param lines
     * @return
     * @throws Exception
     */
    public static List<String> validateJsonSchema(List<String> lines, boolean flagFilter) throws Exception {
        if (null == lines || lines.size() == 0) {
            return null;
        }

        List<String> validatedLines = new ArrayList<String>();

        try {
            for (String jsonString : lines) {
                if (null == jsonString || jsonString.isEmpty()) {
                    continue;
                }

                JSONObject logs;
                try {
                    logs = new JSONObject(jsonString);

                    JSONObject payload = (JSONObject) logs.get("payload");

                    ValidationResult validationResult = JsonSchemaValidator.validateLogRecord(payload);
                    if (flagFilter == validationResult.isSuccess()) {
                        validatedLines.add(jsonString);
                    }
                } catch (JSONException e) {
                    validatedLines.add(jsonString);
                }
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return validatedLines;
    }

    /**
     * saveLines
     * 
     * @param lines
     * @param outputPath
     * @throws Exception
     */
    public static void saveLines(List<String> lines, String outputPath) throws Exception {
        if (null == outputPath || outputPath.isEmpty()) {
            return;
        }

        if (null == lines || lines.size() == 0) {
            return;
        }

        File file = new File(outputPath);
        file.createNewFile();

        BufferedWriter bw = null;

        try {
            bw = new BufferedWriter(new FileWriter(outputPath));

            for (String line : lines) {
                bw.write(line);
                bw.newLine();
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            bw.close();
        }
    }
}