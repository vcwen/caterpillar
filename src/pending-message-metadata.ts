export class PendingMessageMetadata {
  constructor(
    public id: string,
    public consumer: string,
    public timeElapsed: number,
    public deliverTimes: number
  ) {}
}
