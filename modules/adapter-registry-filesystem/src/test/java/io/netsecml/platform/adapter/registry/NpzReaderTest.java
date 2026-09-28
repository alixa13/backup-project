package io.netsecml.platform.adapter.registry;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The delivered calibration scores read exactly as NumPy wrote them, and
// anything that is not a 1-D little-endian float64 array is refused.
class NpzReaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v2", "calibration.npz");

    // One .npy file: magic, version, header length, the header padded with
    // spaces to a multiple of 64 bytes and ending in a newline, then the data.
    private static byte[] npy(int major, String header, double... values) {
        int lengthBytes = major == 1 ? 2 : 4;
        int unpadded = 8 + lengthBytes + header.length() + 1;
        String padded = header + " ".repeat((unpadded + 63) / 64 * 64 - unpadded) + "\n";
        ByteBuffer b = ByteBuffer.allocate(8 + lengthBytes + padded.length() + values.length * 8)
            .order(ByteOrder.LITTLE_ENDIAN);
        b.put(new byte[]{(byte) 0x93, 'N', 'U', 'M', 'P', 'Y'}).put((byte) major).put((byte) 0);
        if (major == 1) {
            b.putShort((short) padded.length());
        } else {
            b.putInt(padded.length());
        }
        b.put(padded.getBytes(StandardCharsets.ISO_8859_1));
        for (double v : values) {
            b.putDouble(v);
        }
        return b.array();
    }

    private static String header(String descr, String fortran, String shape) {
        return "{'descr': '" + descr + "', 'fortran_order': " + fortran + ", 'shape': " + shape + ", }";
    }

    // v2's calibration scores (spec amendment A1): one array per group, as NumPy wrote them.
    @Test
    void theReleasedFileReadsAsNumPyWroteIt() throws Exception {
        Map<String, double[]> arrays = NpzReader.read(Files.readAllBytes(FIXTURE));
        assertEquals(List.of("RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST"),
            new ArrayList<>(arrays.keySet()));
        assertEquals(32760, arrays.get("RESPONSE").length);
        assertEquals(31978, arrays.get("READ_REQUEST").length);
        assertEquals(782, arrays.get("WRITE_REQUEST").length);
        assertEquals(0, arrays.get("OTHER_REQUEST").length);
        assertEquals(8.192552627406258e-07, arrays.get("RESPONSE")[0]);
        assertEquals(8.193268854483904e-07, arrays.get("RESPONSE")[1]);
        assertEquals(1.1041724974347744e-06, arrays.get("READ_REQUEST")[1]);
        assertEquals(1.334367084382393e-06, arrays.get("WRITE_REQUEST")[2]);
    }

    @Test
    void aVersionTwoHeaderReads() {
        assertArrayEquals(new double[]{1.5, -2.0},
            NpzReader.npy("a.npy", npy(2, header("<f8", "False", "(2,)"), 1.5, -2.0)));
    }

    @Test
    void anythingButA1DLittleEndianFloat64ArrayIsRefused() {
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header(">f8", "False", "(1,)"), 1.0)), "big-endian");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f4", "False", "(1,)"), 1.0)), "float32");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "False", "(1, 2)"), 1.0, 2.0)), "2-D");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "True", "(1,)"), 1.0)), "Fortran order");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "False", "(3,)"), 1.0, 2.0)), "truncated");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", "not numpy".getBytes(StandardCharsets.US_ASCII)), "no magic");
    }

    @Test
    void anEntryThatIsNotAnNpyArrayIsRefused() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("notes.txt"));
            zip.write("hello".getBytes(StandardCharsets.US_ASCII));
            zip.closeEntry();
        }
        assertThrows(IllegalStateException.class, () -> NpzReader.read(bytes.toByteArray()));
    }
}
