export default function Panel({ selection, onClose }) {
    if (!selection) return null;

    return (
        <div
            className="fixed z-10 bottom-0 left-0 right-0
                       sm:bottom-4 sm:left-3 sm:top-auto sm:right-auto sm:w-md
                       max-h-[60dvh] sm:max-h-[calc(100vh-6rem)]
                       overflow-y-auto overscroll-contain
                       bg-surface font-sans text-base p-5
                       rounded-t-2xl sm:rounded-xl shadow-xl
                       touch-none sm:touch-auto"
            style={{
                paddingBottom: 'calc(1rem + env(safe-area-inset-bottom))'
            }}
            onPointerDown={e => e.stopPropagation()}
            onPointerMove={e => e.stopPropagation()}
        >
            <div className="text-lg font-semibold">{selection.line1}</div>
            {selection.line2 && <div className="italic font-medium leading-tight pr-5">{selection.line2}</div>}
            <div className="text-muted text-sm mt-2">
                <span className="uppercase tracking-wider text-[10px] bg-muted/10 px-1.5 py-0.5 rounded">
                    {selection.entityType}
                </span>
            </div>
        </div>
    );
}