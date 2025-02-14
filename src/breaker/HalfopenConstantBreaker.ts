import { FailureReason } from "../Policy";
import { EventEmitter } from "../common/Event";
import { FailureOrSuccess } from "../common/Executor";
import { BrokenCircuitError } from '../errors/BrokenCircuitError';
import { SaturationCircuitError } from "../errors/SaturationCircuitError";
import { IHalfOpenBreaker } from "./Breaker";

enum HalfOpenBreakerState {
  Pending,
  Saturated,
  Failed,
  Settled
}

export class HalfopenConstantBreaker implements IHalfOpenBreaker {
  private successCount = 0;
  public halfOpenPromises: Array<Promise<FailureOrSuccess<unknown>>> = [];

  private readonly successEmitter = new EventEmitter<void>();
  private readonly failureEmitter = new EventEmitter<{ lastFailure: FailureReason<unknown>; signal: AbortSignal; }>();
  private readonly settleEmitter = new EventEmitter<void>();

  private state: HalfOpenBreakerState = HalfOpenBreakerState.Pending;

  /**
   * Event emitted when the recovery exceeds the success limit.
   */
  public readonly onSuccess = this.successEmitter.addListener;

  public readonly onFailure = this.failureEmitter.addListener;

  public readonly onSettle = this.settleEmitter.addListener;

  /**
   * HalfopenConstantBreaker recovers if more than `threshold` successes in a row has been received..
   */
  constructor(private readonly threshold: number = 1) {}

  public accept<T>(
    fn: (signal: AbortSignal) => Promise<FailureOrSuccess<T>>,
    signal: AbortSignal
  ): Promise<FailureOrSuccess<T>> | null {
    switch (this.state) {
      case HalfOpenBreakerState.Pending:
        const halfOpenPromiseCount = this.halfOpenPromises.length;
        
        if (halfOpenPromiseCount > this.threshold - 1) {
          this.state = HalfOpenBreakerState.Saturated;
          return null;
        }

        const fnPromise = fn(signal).then((res) => {
          if (!('success' in res)) {
            this.state = HalfOpenBreakerState.Failed;

            this.failureEmitter.emit({ lastFailure: res, signal });
          }
          return res;
        });

        this.halfOpenPromises.push(fnPromise);
        if (halfOpenPromiseCount + 1 === this.threshold) {
          this.calcState(signal);
        }

        return fnPromise;
      case HalfOpenBreakerState.Saturated:
        throw new SaturationCircuitError();

      case HalfOpenBreakerState.Failed:
        throw new BrokenCircuitError();
      case HalfOpenBreakerState.Settled:
        throw new Error('implementation error.')
    }
  }

  private calcState = async (signal: AbortSignal): Promise<void> => {
    // Wait for all fetch promises to settle (resolve or reject)
    const results = await Promise.allSettled(this.halfOpenPromises);
    this.settleEmitter.emit();

    let lastFailure: FailureReason<unknown> | null = null;

    for (let index = 0; index < results.length; index++) {
      const res = results[index];    
      if (res.status === "fulfilled") {
        if ('success' in res.value) {
          this.successCount++;
        } else {
          lastFailure = res.value;
        }
      }
    }
    
    if (this.successCount >= this.threshold) {
      this.state = HalfOpenBreakerState.Settled;
      this.successEmitter.emit();
      this.reset();
    } else if (lastFailure !== null) {
      this.state = HalfOpenBreakerState.Failed;
      this.failureEmitter.emit({ lastFailure: lastFailure!, signal });
      this.reset();
    }
  }

  public reset(): void {
    this.successCount = 0;
    this.halfOpenPromises = [];
    this.state = HalfOpenBreakerState.Pending;
  }
}