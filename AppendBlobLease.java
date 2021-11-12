/**
 * 
 */
package com.test.blob;

import static com.test.blob.BlobUtils.BLOB_SAS_SERVICE;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.mule.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

public class AppendBlobLease {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendBlobLease.class);

    public static final String LOCKED = "LOCKED";
    public static final String TEST_MESSAGE = "Hello IPT, How are you doing?";
    public static final String BLOB_CONTAINER_NAME = "abhi-test";

    public static void main(String[] args) {

        String blobName = "append/" + UUID.getUUID() + ".txt";

        LOGGER.info("** Append Blob Start **");
        BlobContainerClient blobContainerClient = new BlobContainerClientBuilder().endpoint(BLOB_SAS_SERVICE).containerName(BLOB_CONTAINER_NAME).buildClient();

        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
        AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();

        if (appendBlobClient.exists()) {
            LOGGER.info("Append Blob Available - '{}'", blobName);
        } else {
            appendBlobClient.create(false);
            LOGGER.info("New Append Blob created '{}'", blobName);
        }

        // Append blob without leaseID
        appendBlob(blobName, null);

        String leaseId = acquireLease(blobName, 50);
        LOGGER.info("Lease applied on blob '{}' with leaseId '{}'", blobName, leaseId);

        // Append blob with leaseID
        appendBlob(blobName, leaseId);

        LOGGER.info("** Append Blob END ***");

    }

    private static void appendBlob(String blobName, String leaseId) {
        BlobContainerClient blobContainerClient = new BlobContainerClientBuilder().endpoint(BLOB_SAS_SERVICE).containerName(BLOB_CONTAINER_NAME).buildClient();
        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
        AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();

        InputStream blobInputStream = new ByteArrayInputStream(TEST_MESSAGE.getBytes());
        int blobLength = TEST_MESSAGE.length();

        if (leaseId == null) {
            appendBlobClient.appendBlock(blobInputStream, blobLength);
            LOGGER.info("Append Blob '{}'  without leaseID", blobName);
        } else {
            AppendBlobRequestConditions appendBlobRequestConditions = new AppendBlobRequestConditions();
            appendBlobRequestConditions.setLeaseId(leaseId);
            appendBlobClient.appendBlockWithResponse(blobInputStream, blobLength, null, appendBlobRequestConditions, null, null);
            LOGGER.info("Append Blob '{}'  with leaseID '{}'", blobName, leaseId);
        }

        /**
         * Need input from Microsoft,
         * 
         * As per "BlobClientBase.setHttpHeaders" documentation - Changes a blob's
         * HTTP header properties. if only one HTTP header is updated, the others will all be erased. In
         * order to preserve existing values, they must be passed alongside the header being changed.
         * 
         * With keeping above in mind,  
         * 1. We should have option "getHtthHeaders", so we can preserve existing HtthHeaders and then update headers accordingly
         * 2. Otherwise we should have option to update only header which should not wipe out other header
         * 3. BlobClient.uploadWithResponse have option to pass BlobHttpHeaders but AppendBlobClient.appendBlockWithResponse doesn't have this option. 
         *      Can't we have same option with appendBlockWithResponse? 
         * 
         */
        // Update content type
        appendBlobClient.setHttpHeaders(new BlobHttpHeaders().setContentType("text/plain"));

    }

    private static String acquireLease(String blobName, int leaseDuration) {
        BlobContainerClient blobContainerClient = new BlobContainerClientBuilder().endpoint(BLOB_SAS_SERVICE).containerName(BLOB_CONTAINER_NAME).buildClient();
        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();
        String leaseId = blobLeaseClient.acquireLease(leaseDuration);
        return leaseId;
    }



}
