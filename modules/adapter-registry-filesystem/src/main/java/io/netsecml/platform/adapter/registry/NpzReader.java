package io.netsecml.platform.adapter.registry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

// Reads a NumPy .npz archive of 1-D little-endian float64 arrays -- the
// delivered calibration scores, read as delivered (spec section 9, D7). An
// .npz is a zip of .npy files; each is the magic "\x93NUMPY", a version, a
// header length, a Python dict literal header, then the raw values. Anything
// else is refused, never guessed at.
final class NpzReader {

    private static final byte[] MAGIC = {(byte) 0x93, 'N', 'U', 'M', 'P', 'Y'};
    private static final Pattern DESCR = Pattern.compile("'descr':\\s*'([^']*)'");
    private static final Pattern FORTRAN = Pattern.compile("'fortran_order':\\s*(True|False)");
    private static final Pattern SHAPE = Pattern.compile("'shape':\\s*\\(\\s*(\\d+)\\s*,\\s*\\)");

    private NpzReader() {
    }

    // Every array in the archive, by entry name without ".npy", in zip order.
    static Map<String, double[]> read(byte[] npz) throws IOException {
        Map<String, double[]> arrays = new LinkedHashMap<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(npz))) {
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                String name = entry.getName();
                if (!name.endsWith(".npy")) {
                    throw new IllegalStateException("calibration.npz holds " + name + ", not an .npy array");
                }
                arrays.put(name.substring(0, name.length() - ".npy".length()), npy(name, zip.readAllBytes()));
            }
        }
        if (arrays.isEmpty()) {
            throw new IllegalStateException("calibration.npz holds no arrays");
        }
        return arrays;
    }

    // One .npy file: version 1.0 (a 2-byte header length) or 2.0/3.0 (4-byte).
    static double[] npy(String name, byte[] bytes) {
        if (bytes.length < 10 || !Arrays.equals(Arrays.copyOf(bytes, MAGIC.length), MAGIC)) {
            throw new IllegalStateException(name + " is not an .npy array");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int major = bytes[6];
        int offset = major == 1 ? 10 : 12;
        if (major < 1 || major > 3 || bytes.length < offset) {
            throw new IllegalStateException(name + ": unsupported .npy version " + major);
        }
        int headerLength = major == 1 ? buffer.getShort(8) & 0xFFFF : buffer.getInt(8);
        if (headerLength < 0 || offset + headerLength > bytes.length) {
            throw new IllegalStateException(name + ": its header runs past the end of the file");
        }
        String header = new String(bytes, offset, headerLength, StandardCharsets.ISO_8859_1).trim();
        // Exactly one layout: little-endian float64, C order, one dimension.
        Matcher descr = DESCR.matcher(header);
        Matcher fortran = FORTRAN.matcher(header);
        Matcher shape = SHAPE.matcher(header);
        if (!descr.find() || !descr.group(1).equals("<f8") || !fortran.find() || !fortran.group(1).equals("False")
                || !shape.find()) {
            throw new IllegalStateException(name + " is not a 1-D little-endian float64 array: " + header);
        }
        long count = Long.parseLong(shape.group(1));
        int data = offset + headerLength;
        if (bytes.length - data != count * Double.BYTES) {
            throw new IllegalStateException(name + ": its header says " + count + " values, but "
                + (bytes.length - data) + " bytes follow");
        }
        double[] values = new double[(int) count];
        buffer.position(data);
        buffer.asDoubleBuffer().get(values);
        return values;
    }
}
