import {MAP_ET2LAYER} from "./layers.js";
import colors from "./theme.js";

export function Badge({ entityType }) {
    return (
        <span
            className="uppercase tracking-wider text-[10px] px-1.5 py-0.5 rounded font-semibold"
            style={{
                color: MAP_ET2LAYER[entityType].color ?? colors.muted,
                background: "rgba(255,255,255,0.07)",
            }}
        >
            {entityType}
        </span>
    );
}