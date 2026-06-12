<script setup>
// Decorative hero background: a "beacon" light + a drifting data-point network.
// Points/lines are hand-placed in a 1200×600 viewBox.
const points = [
  [80, 90], [210, 180], [150, 320], [60, 470], [300, 90], [330, 260],
  [420, 410], [250, 500], [470, 150], [560, 300], [620, 470], [510, 540],
  [690, 110], [760, 260], [840, 420], [720, 540], [900, 170], [970, 330],
  [1040, 470], [890, 540], [1120, 100], [1150, 300], [1080, 540],
  [390, 200], [600, 90], [820, 90], [1010, 90], [180, 540],
]
const links = [
  [0, 1], [1, 2], [2, 3], [1, 23], [23, 4], [4, 24], [23, 5], [5, 6],
  [6, 7], [5, 8], [8, 9], [9, 10], [10, 11], [9, 13], [12, 13], [13, 14],
  [14, 15], [13, 16], [16, 17], [17, 18], [18, 19], [16, 20], [20, 21],
  [21, 22], [24, 25], [25, 26], [26, 20], [3, 27],
]

// Data packets travelling between beacons — a subset of links, alternating
// direction, staggered so only a few are visible at once.
const packets = links
  .filter((_, i) => i % 3 === 0)
  .map((l, i) => {
    const fwd = i % 2 === 0
    const a = points[l[fwd ? 0 : 1]]
    const b = points[l[fwd ? 1 : 0]]
    return {
      x: a[0], y: a[1],
      dx: b[0] - a[0], dy: b[1] - a[1],
      delay: +(i * 0.8).toFixed(2),
      dur: 3.4 + (i % 4) * 0.7,
    }
  })
</script>

<template>
  <div class="hero-backdrop" aria-hidden="true">
    <!-- beacon light -->
    <div class="hb-sweep"></div>
    <div class="hb-glow"></div>

    <!-- data-point network -->
    <svg class="hb-net" viewBox="0 0 1200 600" preserveAspectRatio="xMidYMid slice">
      <g class="hb-drift">
        <line v-for="(l, i) in links" :key="'l' + i" class="hb-line"
              :x1="points[l[0]][0]" :y1="points[l[0]][1]"
              :x2="points[l[1]][0]" :y2="points[l[1]][1]" />
        <circle v-for="(p, i) in points" :key="'p' + i" class="hb-dot"
                :cx="p[0]" :cy="p[1]" :r="i % 4 === 0 ? 3.4 : 2.2"
                :style="{ animationDelay: (i * 0.27) + 's' }" />
        <circle v-for="(pk, i) in packets" :key="'k' + i" class="hb-packet"
                :cx="pk.x" :cy="pk.y" r="1.8"
                :style="{ '--dx': pk.dx + 'px', '--dy': pk.dy + 'px',
                          animationDelay: pk.delay + 's', animationDuration: pk.dur + 's' }" />
      </g>
    </svg>
  </div>
</template>

<style scoped>
.hero-backdrop {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 760px;
  z-index: 0;
  overflow: hidden;
  pointer-events: none;
  /* fade out toward the bottom so it blends into the page */
  -webkit-mask-image: linear-gradient(to bottom, #000 58%, transparent 100%);
  mask-image: linear-gradient(to bottom, #000 58%, transparent 100%);
}

/* ---- beacon light ---- */
.hb-glow {
  position: absolute;
  top: -160px;
  left: 8%;
  width: 720px;
  height: 720px;
  border-radius: 50%;
  background: radial-gradient(
    circle at center,
    color-mix(in srgb, var(--vp-c-brand-1) 30%, transparent) 0%,
    color-mix(in srgb, #24c6dc 18%, transparent) 38%,
    transparent 68%
  );
  filter: blur(8px);
  opacity: 0.6;
  animation: hb-pulse 7s ease-in-out infinite;
}

.hb-sweep {
  position: absolute;
  top: -360px;
  left: -120px;
  width: 900px;
  height: 900px;
  background: conic-gradient(
    from 0deg,
    transparent 0deg,
    color-mix(in srgb, var(--vp-c-brand-1) 22%, transparent) 18deg,
    transparent 42deg,
    transparent 360deg
  );
  opacity: 0.07;
  animation: hb-rotate 22s linear infinite;
}

@keyframes hb-pulse {
  0%, 100% { opacity: 0.5; transform: scale(1); }
  50%      { opacity: 0.72; transform: scale(1.06); }
}
@keyframes hb-rotate {
  to { transform: rotate(360deg); }
}

/* ---- data-point network ---- */
.hb-net {
  position: absolute;
  top: -50px;          /* clear the nav / menu bar */
  left: 0;
  right: 0;
  bottom: 0;
  opacity: 0.5;
}
.hb-drift {
  transform-origin: center;
  animation: hb-driftmove 32s ease-in-out infinite alternate;
}
.hb-line {
  stroke: color-mix(in srgb, var(--vp-c-brand-1) 45%, transparent);
  stroke-width: 1;
  opacity: 0.35;
}
.hb-dot {
  fill: var(--vp-c-brand-1);
  filter: drop-shadow(0 0 3px color-mix(in srgb, #24c6dc 70%, transparent));
  animation: hb-twinkle 4.5s ease-in-out infinite;
}
.hb-dot:nth-child(3n) { fill: #24c6dc; }

/* data packets travelling between beacons */
.hb-packet {
  fill: #24c6dc;
  filter: drop-shadow(0 0 3px color-mix(in srgb, #24c6dc 80%, transparent));
  animation-name: hb-travel;
  animation-timing-function: linear;
  animation-iteration-count: infinite;
}

@keyframes hb-driftmove {
  0%   { transform: translate(0, 0); }
  100% { transform: translate(-26px, 18px); }
}
@keyframes hb-twinkle {
  0%, 100% { opacity: 0.55; }
  50%      { opacity: 1; }
}
@keyframes hb-travel {
  0%   { transform: translate(0, 0); opacity: 0; }
  12%  { opacity: 0.85; }
  88%  { opacity: 0.85; }
  100% { transform: translate(var(--dx), var(--dy)); opacity: 0; }
}

.dark .hb-glow { opacity: 0.8; }
.dark .hb-net { opacity: 0.7; }
.dark .hb-sweep { opacity: 0.1; }

@media (max-width: 760px) {
  .hero-backdrop { height: 560px; }
  .hb-glow { width: 480px; height: 480px; left: -40px; }
}

@media (prefers-reduced-motion: reduce) {
  .hb-glow, .hb-sweep, .hb-drift, .hb-dot { animation: none; }
  .hb-packet { display: none; }
}
</style>
