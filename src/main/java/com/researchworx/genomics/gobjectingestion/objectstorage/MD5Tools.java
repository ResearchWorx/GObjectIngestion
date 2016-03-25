package com.researchworx.genomics.gobjectingestion.objectstorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;

class MD5Tools {
    private String group;

    MD5Tools(String group) {
        this.group = group;
    }

    String getMultiCheckSum(String fileName) throws IOException {
        String mpHash = null;
        FileInputStream fis = null;

        try {
            List<String> hashList = new ArrayList<>();

            MessageDigest md = MessageDigest.getInstance("MD5");

            File inputFile = new File(fileName);
            //int size = 5242880;
            int partSize = Integer.parseInt(PluginEngine.config.getParam(group, "uploadpartsizemb")) * 1024 * 1024;

            //System.out.println("File Size = " + inputFile.length());
            //System.out.println("Part Size = " + partSize);

            fis = new FileInputStream(inputFile);
            // read bytes to the buffer

            //for(int i = 0; i < inputFile.length(); i = i + size)
            boolean isReading = true;
            int bytesRead = 0;
            while (isReading) {
                byte[] bs;

                long remaining = inputFile.length() - bytesRead;
                //System.out.println("remaingin: " + remaining);
                if (remaining > partSize) {
                    bs = new byte[partSize];
                    bytesRead = bytesRead + fis.read(bs, 0, partSize);
                } else {
                    bs = new byte[(int) remaining];
                    bytesRead = bytesRead + fis.read(bs, 0, (int) remaining);
                }
                byte[] hash = md.digest(bs);
                //System.out.println(getMD5(hash));
                hashList.add(getMD5(hash));
                if (bytesRead == inputFile.length()) {
                    isReading = false;
                }
            }
            mpHash = calculateChecksumForMultipartUpload(hashList);
        } catch (Exception ex) {
            System.out.println("MD5Tools : getMultiPartHash Error " + ex.toString());
        } finally {
            assert fis != null;
            fis.close();
        }
        return mpHash;
    }

    private static String calculateChecksumForMultipartUpload(List<String> md5s) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String md5 : md5s) {
            stringBuilder.append(md5);
        }

        String hex = stringBuilder.toString();
        byte raw[] = BaseEncoding.base16().decode(hex.toUpperCase());
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putBytes(raw);
        String digest = hasher.hash().toString();

        return digest + "-" + md5s.size();
    }

    private static String getMD52(byte[] hash) {
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putBytes(hash);
        return hasher.hash().toString();
    }

    private static String getMD5(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte hashByte : hash) {
            //for (int i = 0; i < hash.length; i++) {
            if ((0xff & /*hash[i]*/ hashByte) < 0x10) {
                hexString.append("0");
                hexString.append(Integer.toHexString(0xFF & /*hash[i]*/ hashByte));
            } else {
                hexString.append(Integer.toHexString(0xFF & /*hash[i]*/ hashByte));
            }
        }
        return hexString.toString();
    }

    String getCheckSum(String path) throws IOException {
        String hash = null;
        //FileInputStream fis = null;
        try (FileInputStream fis = new FileInputStream(new File(path))) {
            //fis = new FileInputStream(new File(path));
            hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        } catch (Exception ex) {
            System.out.println("MD5Tools : getCheckSum Error : " + ex.toString());
        }/* finally {
            fis.close();
		}*/
        return hash;

    }

    public String getCheckSum2(String path) {
        String checksum = null;
        try {
            FileInputStream fis = new FileInputStream(path);
            MessageDigest md = MessageDigest.getInstance("MD5");

            //Using MessageDigest update() method to provide input
            byte[] buffer = new byte[8192];
            int numOfBytesRead;
            while ((numOfBytesRead = fis.read(buffer)) > 0) {
                md.update(buffer, 0, numOfBytesRead);
            }
            byte[] hash = md.digest();
            checksum = new BigInteger(1, hash).toString(16); //don't use this, truncates leading zero
            fis.close();
        } catch (Exception ex) {
            System.out.println("ObjectEngine : checkSum");
        }
        return checksum;
    }


}
