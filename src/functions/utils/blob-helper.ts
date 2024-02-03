import { BlobClient,  BlobSASPermissions, BlobServiceClient, ContainerClient, generateBlobSASQueryParameters, SASProtocol, UserDelegationKey } from '@azure/storage-blob'

import { DefaultAzureCredential } from '@azure/identity'
import { config } from './config'

export async function generateSourceSasToken(
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
      config.sourceBlobStorageUri
    )
    const userDelegationKey = await sourceBlobServiceClient.getUserDelegationKey(
      new Date(),
      new Date(new Date().valueOf() + 3600 * 1000)
    )
    return userDelegationKey
  }
  
  
  export function createBlobContainerClient(
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
  