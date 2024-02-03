import * as sql from 'mssql'
import { log } from './logger'
import { DefaultAzureCredential } from '@azure/identity'

export async function addRecord(fileName: string): Promise<void> {
  const token = await getToken()
  log('token', { token })
  const connection = await sql.connect(
    `Server=meng-db-server.database.windows.net;Database=meng-db;Authentication=Active Directory Integrated;token=${token};Encrypt=true`
  )
  await connection
    .request()
    .input('FileName', sql.VarChar(255), fileName)
    .input('CreatedDate', sql.DateTime, new Date())
    .query(
      'INSERT INTO FilesCopied (FileName, CreatedDate) VALUES (@FileName, @CreatedDate)'
    )

  await connection.close()

  log('Record added successfully!')
}

async function getToken(): Promise<string> {
  const credential = new DefaultAzureCredential()
  const tokenResponse = await credential.getToken(
    'https://database.windows.net/.default'
  )

  const token = tokenResponse?.token

  if (!token) {
    throw new Error('Failed to acquire access token.')
  }

  return token
}
