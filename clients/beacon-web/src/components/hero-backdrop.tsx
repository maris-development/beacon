/**
 * Decorative animated backdrop — a "beacon" light plus a drifting data-point
 * network. Ported from the Beacon docs homepage (HeroBackdrop.vue). Purely
 * decorative; respects `prefers-reduced-motion` via CSS.
 */

// Points / links hand-placed in a 1200×600 viewBox.
const POINTS: [number, number][] = [
  [80, 90], [210, 180], [150, 320], [60, 470], [300, 90], [330, 260],
  [420, 410], [250, 500], [470, 150], [560, 300], [620, 470], [510, 540],
  [690, 110], [760, 260], [840, 420], [720, 540], [900, 170], [970, 330],
  [1040, 470], [890, 540], [1120, 100], [1150, 300], [1080, 540],
  [390, 200], [600, 90], [820, 90], [1010, 90], [180, 540],
];
const LINKS: [number, number][] = [
  [0, 1], [1, 2], [2, 3], [1, 23], [23, 4], [4, 24], [23, 5], [5, 6],
  [6, 7], [5, 8], [8, 9], [9, 10], [10, 11], [9, 13], [12, 13], [13, 14],
  [14, 15], [13, 16], [16, 17], [17, 18], [18, 19], [16, 20], [20, 21],
  [21, 22], [24, 25], [25, 26], [26, 20], [3, 27],
];

const PACKETS = LINKS.filter((_, i) => i % 3 === 0).map((l, i) => {
  const fwd = i % 2 === 0;
  const a = POINTS[l[fwd ? 0 : 1]];
  const b = POINTS[l[fwd ? 1 : 0]];
  return {
    x: a[0],
    y: a[1],
    dx: b[0] - a[0],
    dy: b[1] - a[1],
    delay: +(i * 0.8).toFixed(2),
    dur: 3.4 + (i % 4) * 0.7,
  };
});

export function HeroBackdrop() {
  return (
    <div className="beacon-backdrop" aria-hidden="true">
      <div className="hb-sweep" />
      <div className="hb-glow" />
      <svg className="hb-net" viewBox="0 0 1200 600" preserveAspectRatio="xMidYMid slice">
        <g className="hb-drift">
          {LINKS.map((l, i) => (
            <line
              key={`l${i}`}
              className="hb-line"
              x1={POINTS[l[0]][0]}
              y1={POINTS[l[0]][1]}
              x2={POINTS[l[1]][0]}
              y2={POINTS[l[1]][1]}
            />
          ))}
          {POINTS.map((p, i) => (
            <circle
              key={`p${i}`}
              className="hb-dot"
              cx={p[0]}
              cy={p[1]}
              r={i % 4 === 0 ? 3.4 : 2.2}
              style={{ animationDelay: `${i * 0.27}s` }}
            />
          ))}
          {PACKETS.map((pk, i) => (
            <circle
              key={`k${i}`}
              className="hb-packet"
              cx={pk.x}
              cy={pk.y}
              r={1.8}
              style={
                {
                  "--dx": `${pk.dx}px`,
                  "--dy": `${pk.dy}px`,
                  animationDelay: `${pk.delay}s`,
                  animationDuration: `${pk.dur}s`,
                } as React.CSSProperties
              }
            />
          ))}
        </g>
      </svg>
    </div>
  );
}
