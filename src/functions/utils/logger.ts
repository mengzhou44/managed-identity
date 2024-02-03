import * as appInsights from 'applicationinsights'
import { config } from './config'

appInsights.setup(config.appInsightsKey).start()
const appInsightsClient = appInsights.defaultClient

export function log(message: string, meta: any = null) {
    appInsightsClient.trackTrace({
      message,
      properties: { ...meta, applicationName: 'trrf/admin', userId: 'meng.zhou' },
    })
  }