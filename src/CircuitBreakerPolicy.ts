import { ConstantBackoff, IBackoff, IBackoffFactory } from './backoff/Backoff';
import { IBreaker, IHalfOpenBreaker } from './breaker/Breaker';
import { HalfopenConstantBreaker } from './breaker/HalfopenConstantBreaker';
import { neverAbortedSignal } from './common/abort';
import { EventEmitter } from './common/Event';
import { ExecuteWrapper, returnOrThrow } from './common/Executor';
import { BrokenCircuitError, HydratingCircuitError, isBrokenCircuitError, isSaturationCircuitError, SaturationCircuitError } from './errors/Errors';
import { IsolatedCircuitError } from './errors/IsolatedCircuitError';
import { FailureReason, IDefaultPolicyContext, IPolicy } from './Policy';

export enum CircuitState {
  /**
   * Normal operation. Execution of actions allowed.
   */
  Closed,

  /**
   * The automated controller has opened the circuit. Execution of actions blocked.
   */
  Open,

  /**
   * Recovering from open state, after the automated break duration has
   * expired. Execution of actions permitted. Success of subsequent action/s
   * controls onward transition to Open or Closed state.
   */
  HalfOpen,

  /**
   * Circuit held manually in an open state. Execution of actions blocked.
   */
  Isolated,
}

/**
 * Context passed into halfOpenAfter backoff delegate.
 */
export interface IHalfOpenAfterBackoffContext extends IDefaultPolicyContext {
  /**
   * The consecutive number of times the circuit has entered the
   * {@link CircuitState.Open} state.
   */
  attempt: number;
  /**
   * The result of the last method call that caused the circuit to enter the
   * {@link CircuitState.Open} state. Either a thrown error, or a value that we
   * determined should open the circuit.
   */
  result: FailureReason<unknown>;
}

export interface ICircuitBreakerOptions {
  breaker: IBreaker;

  /**
   * When to (potentially) enter the {@link CircuitState.HalfOpen} state from
   * the {@link CircuitState.Open} state. Either a duration in milliseconds or a
   * backoff factory.
   */
  halfOpenAfter: number | IBackoffFactory<IHalfOpenAfterBackoffContext>;

  /**
   * Initial state from a previous call to {@link CircuitBreakerPolicy.toJSON}.
   */
  initialState?: unknown;

  halfOpenBreaker?: IHalfOpenBreaker;
}

type InnerState =
  | { value: CircuitState.Closed }
  | { value: CircuitState.Isolated; counters: number }
  | {
      value: CircuitState.Open;
      openedAt: number;
      attemptNo: number;
      backoff: IBackoff<IHalfOpenAfterBackoffContext>;
    }
  | {
      value: CircuitState.HalfOpen;
      attemptNo: number;
      backoff: IBackoff<IHalfOpenAfterBackoffContext>;
      probeNo: number;
    };

interface ISerializedState {
  ownState: Partial<InnerState>;
  breakerState: unknown;
}

export class CircuitBreakerPolicy implements IPolicy {
  declare readonly _altReturn: never;

  public halfOpenBreaker: IHalfOpenBreaker;
  private readonly breakEmitter = new EventEmitter<FailureReason<unknown> | { isolated: true }>();
  private readonly resetEmitter = new EventEmitter<void>();
  private readonly halfOpenEmitter = new EventEmitter<void>();
  private readonly stateChangeEmitter = new EventEmitter<CircuitState>();
  private readonly halfOpenAfterBackoffFactory: IBackoffFactory<IHalfOpenAfterBackoffContext>;
  private innerLastFailure?: FailureReason<unknown>;
  private innerState: InnerState = { value: CircuitState.Closed };

  /**
   * Event emitted when the circuit breaker opens.
   */
  public readonly onBreak = this.breakEmitter.addListener;

  /**
   * Event emitted when the circuit breaker resets.
   */
  public readonly onReset = this.resetEmitter.addListener;

  /**
   * Event emitted when the circuit breaker is half open (running a test call).
   * Either `onBreak` on `onReset` will subsequently fire.
   */
  public readonly onHalfOpen = this.halfOpenEmitter.addListener;

  /**
   * Fired whenever the circuit breaker state changes.
   */
  public readonly onStateChange = this.stateChangeEmitter.addListener;

  /**
   * @inheritdoc
   */
  public readonly onSuccess = this.executor.onSuccess;

  /**
   * @inheritdoc
   */
  public readonly onFailure = this.executor.onFailure;

  /**
   * Gets the current circuit breaker state.
   */
  public get state(): CircuitState {
    return this.innerState.value;
  }

  /**
   * Gets the last reason the circuit breaker failed.
   */
  public get lastFailure() {
    return this.innerLastFailure;
  }

  constructor(
    private readonly options: ICircuitBreakerOptions,
    private readonly executor: ExecuteWrapper,
  ) {
    this.halfOpenAfterBackoffFactory =
      typeof options.halfOpenAfter === 'number'
        ? new ConstantBackoff(options.halfOpenAfter)
        : options.halfOpenAfter;

    this.halfOpenBreaker = this.options.halfOpenBreaker || new HalfopenConstantBreaker();

    if (options.initialState) {
      const initialState = options.initialState as ISerializedState;
      this.innerState = initialState.ownState as InnerState;
      this.options.breaker.state = initialState.breakerState;

      if (
        this.innerState.value === CircuitState.Open ||
        this.innerState.value === CircuitState.HalfOpen
      ) {
        this.innerLastFailure = { error: new HydratingCircuitError() };
        let backoff = this.halfOpenAfterBackoffFactory.next({
          attempt: 1,
          result: this.innerLastFailure,
          signal: neverAbortedSignal,
        });
        for (let i = 2; i <= this.innerState.attemptNo; i++) {
          backoff = backoff.next({
            attempt: i,
            result: this.innerLastFailure,
            signal: neverAbortedSignal,
          });
        }
        this.innerState.backoff = backoff;
      }
    }

    this.halfOpenBreaker.onSuccess(() => {
      this.close();
    });

    this.halfOpenBreaker.onFailure(({ lastFailure, signal }) => {
      if (this.innerState.value !== CircuitState.HalfOpen) return;
      this.innerLastFailure = lastFailure;
      this.options.breaker.failure(CircuitState.HalfOpen);
      this.open(lastFailure, signal);
    });
  }
  
  /**
   * Manually holds open the circuit breaker.
   * @returns A handle that keeps the breaker open until `.dispose()` is called.
   */
  public isolate() {
    if (this.innerState.value !== CircuitState.Isolated) {
      this.innerState = { value: CircuitState.Isolated, counters: 0 };
      this.breakEmitter.emit({ isolated: true });
      this.stateChangeEmitter.emit(CircuitState.Isolated);
    }

    this.innerState.counters++;

    let disposed = false;
    return {
      dispose: () => {
        if (disposed) {
          return;
        }

        disposed = true;
        if (this.innerState.value === CircuitState.Isolated && !--this.innerState.counters) {
          this.innerState = { value: CircuitState.Closed };
          this.resetEmitter.emit();
          this.stateChangeEmitter.emit(CircuitState.Closed);
        }
      },
    };
  }

  /**
   * Executes the given function.
   * @param fn Function to run
   * @throws a {@link BrokenCircuitError} if the circuit is open
   * @throws a {@link IsolatedCircuitError} if the circuit is held
   * open via {@link CircuitBreakerPolicy.isolate}
   * @returns a Promise that resolves or rejects with the function results.
   */
  public async execute<T>(
    fn: (context: IDefaultPolicyContext) => PromiseLike<T> | T,
    signal = neverAbortedSignal,
  ): Promise<T> {
    const state = this.innerState;
    switch (state.value) {
      case CircuitState.Closed:
        const result = await this.executor.invoke(fn, { signal });
        if ('success' in result) {
          this.options.breaker.success(state.value);
        } else {
          this.innerLastFailure = result;
          if (this.options.breaker.failure(state.value)) {
            this.open(result, signal);
          }
        }

        return returnOrThrow(result);

      case CircuitState.HalfOpen:
        return this.halfOpen(fn, signal);

      case CircuitState.Open:
        if (Date.now() - state.openedAt < state.backoff.duration) {
          throw new BrokenCircuitError();
        }
        this.halfOpenEmitter.emit();
        this.halfOpenBreaker.reset();

        this.innerState = {
          value: CircuitState.HalfOpen,
          backoff: state.backoff,
          attemptNo: state.attemptNo + 1,
          probeNo: 0
        };
        
        this.stateChangeEmitter.emit(CircuitState.HalfOpen);
        return this.execute(fn, signal);

      case CircuitState.Isolated:
        throw new IsolatedCircuitError();

      default:
        throw new Error(`Unexpected circuit state ${state}`);
    }
  }

  /**
   * Captures circuit breaker state that can later be used to recreate the
   * breaker by passing `state` to the `circuitBreaker` function. This is
   * useful in cases like serverless functions where you may want to keep
   * the breaker state across multiple executions.
   */
  public toJSON(): unknown {
    const state = this.innerState;
    let ownState: Partial<InnerState>;
    if (state.value === CircuitState.HalfOpen) {
      ownState = {
        value: CircuitState.Open,
        openedAt: 0,
        attemptNo: state.attemptNo,
      };
    } else if (state.value === CircuitState.Open) {
      ownState = {
        value: CircuitState.Open,
        openedAt: state.openedAt,
        attemptNo: state.attemptNo,
      };
    } else {
      ownState = state;
    }

    return { ownState, breakerState: this.options.breaker.state } satisfies ISerializedState;
  }

  private async halfOpen<T>(
    fn: (context: IDefaultPolicyContext) => PromiseLike<T> | T,
    signal: AbortSignal,
  ): Promise<T> {
    const executFn = (signal: AbortSignal) => this.executor.invoke(fn, { signal });

    // halfopen breaker accepts a limit amount of functions until it refuses to serve requests.
    const fnPromise = this.halfOpenBreaker.accept(executFn, signal);

    if (fnPromise === null) {
      throw new SaturationCircuitError();
    }

    (this.innerState as {
      value: CircuitState.HalfOpen;
      attemptNo: number;
      backoff: IBackoff<IHalfOpenAfterBackoffContext>;
      probeNo: number;
    }).probeNo++

    try {
      const result = await fnPromise;

      if ('success' in result) {
        this.options.breaker.success(CircuitState.HalfOpen);
      } else {
        this.innerLastFailure = result;
        this.open(result, signal);
        this.options.breaker.failure(CircuitState.HalfOpen);
      }

      if ('error' in result) {
        throw { handledError: result.error };
      }

      return returnOrThrow(result);
    } catch (err: unknown) {
      if (typeof err === 'object' && err !== null && 'handledError' in err) {
        throw err.handledError;
      }

      if (isSaturationCircuitError(err) || isBrokenCircuitError(err)) {
        throw err;
      }
      // It's an error, but not one the circuit is meant to retry, so
      // for our purposes it's a success. Task failed successfully!
      this.close();
      throw err;
    }
  }

  private open(reason: FailureReason<unknown>, signal: AbortSignal) {
    if (this.state === CircuitState.Isolated || this.state === CircuitState.Open) {
      return;
    }

    const attemptNo =
      this.innerState.value === CircuitState.HalfOpen ? this.innerState.attemptNo : 1;
    const context = { attempt: attemptNo, result: reason, signal };
    const backoff =
      this.innerState.value === CircuitState.HalfOpen
        ? this.innerState.backoff.next(context)
        : this.halfOpenAfterBackoffFactory.next(context);

    this.innerState = { value: CircuitState.Open, openedAt: Date.now(), backoff, attemptNo };
    this.breakEmitter.emit(reason);
    this.stateChangeEmitter.emit(CircuitState.Open);
    //this.halfOpenBreaker.reset();
  }

  private close() {
    if (this.state === CircuitState.HalfOpen) {
      this.innerState = { value: CircuitState.Closed };
      this.resetEmitter.emit();
      this.stateChangeEmitter.emit(CircuitState.Closed);
      //this.halfOpenBreaker.reset();
      this.options.breaker.reset();
    }
  }
}
