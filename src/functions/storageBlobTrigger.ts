import { app, InvocationContext } from '@azure/functions'
import { BlobItem, ContainerClient } from '@azure/storage-blob'

import { config } from './utils/config'
import { addRecord } from './utils/add-record-to-db'
import { log } from './utils/logger'

import {
  createBlobContainerClient,
  generateSourceSasToken,
} from './utils/blob-helper'

export async function storageBlobTrigger(
  blob: Buffer,
  context: InvocationContext
): Promise<void> {
  try {
    const sourceContainer = createBlobContainerClient(
      config.sourceBlobStorageUri,
      'test'
    )
    const targetContainer = createBlobContainerClient(
      config.targetBlobStorageUri,
      'target'
    )
    
    const containerExists = await targetContainer.exists()
    if (!containerExists) {
      await targetContainer.create()
    }

    for await (const blob of sourceContainer.listBlobsFlat()) {
      await copyBlobToNewContainer(sourceContainer, targetContainer, blob)
      await addRecord(blob.name)
    }
  } catch (err) {
    log(err.message, err)
  }
}

async function copyBlobToNewContainer(
  sourceContainer: ContainerClient,
  targetContainer: ContainerClient,
  blob: BlobItem
) {
  const sourceBlob = sourceContainer.getBlobClient(blob.name)
  const targetBlob = targetContainer.getBlobClient(blob.name)

  const sourceSasToken = await generateSourceSasToken(
    sourceContainer,
    sourceBlob
  )
  log('source sas token', { sourceSasToken })
  const sourceBlobUrl = `${sourceBlob.url}?${sourceSasToken}`

  const response = await targetBlob.beginCopyFromURL(sourceBlobUrl)
  return await response.pollUntilDone()
}

app.storageBlob('storageBlobTrigger', {
  path: 'test/{name}',
  connection: 'SourceBlobStorage',
  handler: storageBlobTrigger,
})
