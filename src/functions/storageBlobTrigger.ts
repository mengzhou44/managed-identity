import { app, InvocationContext } from '@azure/functions'

import {
  BlobServiceClient,
  ContainerClient,
  BlobClient,
  generateBlobSASQueryParameters,
  BlobSASPermissions,
  SASProtocol,
  UserDelegationKey,
} from '@azure/storage-blob'

import * as appInsights from 'applicationinsights'
import { DefaultAzureCredential } from '@azure/identity'
import { BlobItem } from '@azure/storage-blob'

appInsights.setup(process.env.APP_INSIGHTS_KEY).start()
const appInsightsClient = appInsights.defaultClient

export async function storageBlobTrigger(
  blob: Buffer,
  context: InvocationContext
): Promise<void> {
  const message = `Storage blob function processed blob "${context.triggerMetadata.name}" with size ${blob.length} bytes`
  log(message)
  const sourceContainer = createBlobContainerClient(
    process.env.SourceBlobStorageUri,
    'test'
  )
  const targetContainer = createBlobContainerClient(
    process.env.SourceBlobStorageUri,
    'target'
  )
  const containerExists = await targetContainer.exists()
  if (!containerExists) {
    await targetContainer.create()
  }

  for await (const blob of sourceContainer.listBlobsFlat()) {
    await copyBlobToNewContainer(sourceContainer, targetContainer, blob)
  }

  log('The copying of files has been successfully completed.')
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
  log(sourceSasToken)
  const sourceBlobUrl = `${sourceBlob.url}?${sourceSasToken}`

  const response = await targetBlob.beginCopyFromURL(sourceBlobUrl)
  return await response.pollUntilDone()
}

async function generateSourceSasToken(
  containerClient: ContainerClient,
  blobClient: BlobClient
): Promise<string> {
  const userDelegationKey = await getUserDelegationKey()

  const sasToken = generateBlobSASQueryParameters(
    {
      containerName: containerClient.containerName,
      blobName: blobClient.name,
      permissions: BlobSASPermissions.parse('racwd'),
      startsOn: new Date(),
      expiresOn: new Date(new Date().valueOf() + 3600 * 1000),
      protocol: SASProtocol.Https,
    },
    userDelegationKey,
    process.env.SourceBlobStorageAccountName
  ).toString()

  return sasToken
}

async function getUserDelegationKey(): Promise<UserDelegationKey> {
  const sourceBlobServiceClient = getBlobServiceClient(
    process.env.SourceBlobStorageUri
  )
  const userDelegationKey = await sourceBlobServiceClient.getUserDelegationKey(
    new Date(),
    new Date(new Date().valueOf() + 3600 * 1000)
  )
  return userDelegationKey
}

function createBlobContainerClient(
  uri: string,
  containerName: string
): ContainerClient {
  const client = getBlobServiceClient(uri)
  return client.getContainerClient(containerName)
}

function getBlobServiceClient(uri: string): BlobServiceClient {
  const tokenCredential = new DefaultAzureCredential()
  return new BlobServiceClient(uri, tokenCredential)
}

function log(message: string) {
  appInsightsClient.trackEvent({
    name: '/function',
    properties: { message },
  })
}

app.storageBlob('storageBlobTrigger', {
  path: 'test/{name}',
  connection: 'blobStorage',
  handler: storageBlobTrigger,
})
