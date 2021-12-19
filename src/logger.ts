import {
  createLogger as createWinstonLogger,
  format,
  Logger,
  transports
} from 'winston'

const simpleFormat = format.printf(({ level, message, label }) => {
  return `${level}:[${label}] ${message}`
})

export const createLogger = (label: string): Logger =>
  createWinstonLogger({
    level: 'debug',
    format: format.combine(
      format.label({ label }),
      format.splat(),
      simpleFormat
    ),
    transports: [
      new transports.Console({
        level: process.env.LOG_LEVEL ?? 'info'
      })
    ]
  })
