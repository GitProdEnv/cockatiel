import { CircuitState } from '../CircuitBreakerPolicy';
import { FailureReason } from '../Policy';
import { FailureOrSuccess } from '../common/Executor';

/**
 * The breaker determines when the circuit breaker should open.
 */
export interface IBreaker {
  /**
   * Gets or sets the internal state of the breaker. Used for serialization
   * with {@link CircuitBreaker.toJSON}.
   */
  state: unknown;

  /**
   * Called when a call succeeds.
   */
  success(state: CircuitState): void;

  /**
   * Called when a call fails. Returns true if the circuit should open.
   */
  failure(state: CircuitState): boolean;

  reset(): void;
}

export interface IHalfOpenBreaker {
  accept<T>(
    fn: (signal: AbortSignal) => Promise<FailureOrSuccess<T>>,
    signal: AbortSignal
  ): Promise<FailureOrSuccess<T>> | null;
  onSuccess(cb: () => void): void;
  onFailure(cb: (context: { lastFailure: FailureReason<unknown>; signal: AbortSignal; }) => void): void;
  reset(): void;
}

export * from './ConsecutiveBreaker';
export * from './CountBreaker';
export * from './SamplingBreaker';

