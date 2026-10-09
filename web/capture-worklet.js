// Mic capture for the phone page (CQ-6): resample to 16 kHz mono, 16-bit
// little-endian PCM, cut into fixed-size chunks (3200 bytes = 0.1 s).
//
// Loaded twice from the same file:
//  - as an AudioWorklet module (ctx.audioWorklet.addModule): registers the
//    "pcm-capture" processor, which posts {type: "chunk", buf} messages;
//  - as a classic <script> on the page: exposes Resampler and PcmChunker on
//    globalThis, for the ScriptProcessor fallback and for the browser tests.
// No imports, so it works the same in both places (and in older Safari).

// Streaming windowed-sinc low-pass resampler, any input rate -> outRate.
//
// Each output sample is a weighted sum of the input samples around its
// position, weights = a Blackman-windowed sinc low-pass with its cutoff at
// `cutoff` (default 90%) of the lower of the two Nyquist rates (7.2 kHz for
// 16 kHz output), so content above 8 kHz is filtered out instead of aliasing
// back into the speech band (naive sample dropping would). The kernel is
// precomputed for `phases` fractional positions (polyphase table), so 48 kHz
// -> 16 kHz (exactly 3:1) and 44.1 kHz -> 16 kHz (2.75625:1) both cost about
// 2 * half multiply-adds per output sample. Each phase is normalised to unit
// DC gain. Positions are tracked as exact integers (units of 1/outRate input
// samples), so there is no drift over a long session. Latency: `half` input
// samples (~0.6 ms at 48 kHz).
class Resampler {
  constructor(inRate, outRate = 16000, opts = {}) {
    inRate = Math.round(inRate);
    outRate = Math.round(outRate);
    if (!(inRate > 0 && outRate > 0)) throw new RangeError("sample rates must be positive");
    const zeroCrossings = opts.zeroCrossings || 8;
    const phases = opts.phases || 256;
    const cutoff = opts.cutoff || 0.9;
    this.inRate = inRate;
    this.outRate = outRate;
    this.phases = phases;
    // Cutoff in cycles per input sample.
    const fc = 0.5 * Math.min(1, outRate / inRate) * cutoff;
    const half = Math.max(1, Math.ceil(zeroCrossings / (2 * fc)));
    const taps = 2 * half;
    this.half = half;
    this.taps = taps;
    // Row p holds the weights for an output at fractional position p/phases
    // past input sample `base`; tap k reads input sample base - half + 1 + k.
    // Row `phases` (frac = 1) avoids a special case when rounding up.
    this.table = new Float32Array((phases + 1) * taps);
    for (let p = 0; p <= phases; p++) {
      const frac = p / phases;
      let sum = 0;
      for (let k = 0; k < taps; k++) {
        const t = k - half + 1 - frac; // input sample minus output position, in input samples
        let h = t === 0 ? 2 * fc : Math.sin(2 * Math.PI * fc * t) / (Math.PI * t);
        const x = t / half; // -1..1 across the kernel
        h *= Math.abs(x) >= 1 ? 0 : 0.42 + 0.5 * Math.cos(Math.PI * x) + 0.08 * Math.cos(2 * Math.PI * x);
        this.table[p * taps + k] = h;
        sum += h;
      }
      for (let k = 0; k < taps; k++) this.table[p * taps + k] /= sum;
    }
    // Input history. Starts with half - 1 zeros so the first output sits on
    // the first input sample.
    this.buf = new Float32Array(4096 + taps);
    this.len = half - 1;
    this.pos = (half - 1) * outRate; // next output position in buf, times outRate
    this.out = new Float32Array(0);
  }

  // Feed input samples (Float32Array, -1..1); returns the new output samples
  // (a view that is only valid until the next call).
  process(input) {
    if (this.len + input.length > this.buf.length) {
      const bigger = new Float32Array(Math.max(this.buf.length * 2, this.len + input.length));
      bigger.set(this.buf.subarray(0, this.len));
      this.buf = bigger;
    }
    this.buf.set(input, this.len);
    this.len += input.length;
    const maxOut = Math.ceil((input.length * this.outRate) / this.inRate) + 2;
    if (this.out.length < maxOut) this.out = new Float32Array(maxOut * 2);
    const { buf, table, taps, half, phases, inRate, outRate, out } = this;
    let pos = this.pos;
    let n = 0;
    for (;;) {
      let base = Math.floor(pos / outRate);
      let phase = Math.round(((pos - base * outRate) / outRate) * phases);
      if (base + half >= this.len) break; // needs input we don't have yet
      const row = phase * taps;
      const start = base - half + 1;
      let acc = 0;
      for (let k = 0; k < taps; k++) acc += table[row + k] * buf[start + k];
      out[n++] = acc;
      pos += inRate;
    }
    // Drop input no future output needs.
    const keepFrom = Math.max(0, Math.floor(pos / outRate) - half + 1);
    if (keepFrom > 0) {
      buf.copyWithin(0, keepFrom, this.len);
      this.len -= keepFrom;
      pos -= keepFrom * outRate;
    }
    this.pos = pos;
    return out.subarray(0, n);
  }
}

// Float samples -> 16-bit little-endian PCM, cut into chunks of chunkBytes.
// onChunk(ArrayBuffer) gets each full chunk; flush() returns the partial one.
class PcmChunker {
  constructor(inRate, onChunk, chunkBytes = 3200, outRate = 16000) {
    this.resampler = new Resampler(inRate, outRate);
    this.onChunk = onChunk;
    this.chunkBytes = chunkBytes;
    this._new();
  }

  _new() {
    this.chunk = new ArrayBuffer(this.chunkBytes);
    this.view = new DataView(this.chunk);
    this.fill = 0;
  }

  // channels: array of Float32Array (one per channel); mixed down to mono.
  push(channels) {
    if (!channels || !channels.length || !channels[0]) return;
    let mono = channels[0];
    if (channels.length > 1) {
      const mix = new Float32Array(mono.length);
      for (const ch of channels) for (let i = 0; i < mix.length; i++) mix[i] += ch[i];
      for (let i = 0; i < mix.length; i++) mix[i] /= channels.length;
      mono = mix;
    }
    const out = this.resampler.process(mono);
    for (let i = 0; i < out.length; i++) {
      const s = out[i] > 1 ? 1 : out[i] < -1 ? -1 : out[i];
      this.view.setInt16(this.fill, Math.round(s < 0 ? s * 0x8000 : s * 0x7fff), true);
      this.fill += 2;
      if (this.fill === this.chunkBytes) {
        const full = this.chunk;
        this._new();
        this.onChunk(full);
      }
    }
  }

  // The samples since the last full chunk (possibly empty); starts a new chunk.
  flush() {
    const part = this.chunk.slice(0, this.fill);
    this._new();
    return part;
  }
}

if (typeof AudioWorkletProcessor !== "undefined" && typeof registerProcessor === "function") {
  class PcmCaptureProcessor extends AudioWorkletProcessor {
    constructor(options) {
      super();
      const chunkBytes = (options && options.processorOptions && options.processorOptions.chunkBytes) || 3200;
      // sampleRate: the AudioContext's rate (a global in the worklet scope).
      this.chunker = new PcmChunker(sampleRate, (buf) => this.port.postMessage({ type: "chunk", buf }, [buf]),
                                    chunkBytes);
      this.port.onmessage = (e) => {
        if (e.data === "flush") {
          const buf = this.chunker.flush();
          this.port.postMessage({ type: "flush", buf }, [buf]);
        }
      };
    }

    process(inputs, outputs) {
      this.chunker.push(inputs[0]);
      // Output stays silent (it only exists so the node is pulled by the graph).
      return true;
    }
  }
  registerProcessor("pcm-capture", PcmCaptureProcessor);
} else {
  globalThis.Resampler = Resampler;
  globalThis.PcmChunker = PcmChunker;
}
